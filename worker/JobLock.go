package worker

import (
	"context"
	"crontab/common"
	clientV3 "go.etcd.io/etcd/client/v3"
)

//分布式锁
type JobLock struct {
	Kv         clientV3.KV
	Lease      clientV3.Lease
	JobName    string             //任务名
	CancelFunc context.CancelFunc //用于终止自动续租
	LeaseId    clientV3.LeaseID   //租约ID
	IsLocked   bool               //是否上锁成功
}

//InitJobLock 初始化
func InitJobLock(jobName string, kv clientV3.KV, lease clientV3.Lease) (jobLock *JobLock) {
	jobLock = &JobLock{
		Kv:      kv,
		Lease:   lease,
		JobName: jobName,
	}
	return
}

func (jobLock *JobLock) TryLock() (err error) {
	var (
		leaseGrantResp *clientV3.LeaseGrantResponse
		cancelCtx      context.Context
		cancelFunc     context.CancelFunc
		leaseId        clientV3.LeaseID
		keepRespChan   <-chan *clientV3.LeaseKeepAliveResponse
		txn            clientV3.Txn
		lockKey        string
		txnResp        *clientV3.TxnResponse
	)
	//1.先创建租约（5秒），防止节点宕机导致锁不释放的问题
	if leaseGrantResp, err = jobLock.Lease.Grant(context.TODO(), 5); err != nil {
		return
	}
	//context用于取消自动续租
	cancelCtx, cancelFunc = context.WithCancel(context.TODO())
	leaseId = leaseGrantResp.ID
	//2.自动续租
	if keepRespChan, err = jobLock.Lease.KeepAlive(cancelCtx, leaseId); err != nil {
		goto FAIL
	}
	//3.处理续租应答的协程
	go func() {
		var keepResp *clientV3.LeaseKeepAliveResponse
		for {
			select {
			case keepResp = <-keepRespChan: //自动续租应答
				if keepResp == nil {
					goto END
				}
			}
		}
	END:
	}()

	//4.创建事务txn
	txn = jobLock.Kv.Txn(context.TODO())
	//锁路径
	lockKey = common.JOB_LOCK_DIR + jobLock.JobName
	//5.事务抢锁
	txn.If(clientV3.Compare(clientV3.CreateRevision(lockKey), "=", 0)).
		Then(clientV3.OpPut(lockKey, "", clientV3.WithLease(leaseId))).
		Else(clientV3.OpDelete(lockKey))
	if txnResp, err = txn.Commit(); err != nil {
		goto FAIL
	}
	//6.失败释放租约
	if !txnResp.Succeeded {
		err = common.ERR_LOCK_ALREADY_REQUIRED
		goto FAIL
	}
	//抢锁成功
	jobLock.LeaseId = leaseId
	jobLock.CancelFunc = cancelFunc
	jobLock.IsLocked = true
	return
FAIL:
	cancelFunc()                                  //取消自动续租
	jobLock.Lease.Revoke(context.TODO(), leaseId) //释放租约
	return
}
func (jobLock *JobLock) Unlock() {
	if jobLock.IsLocked {
		jobLock.CancelFunc()
		jobLock.Lease.Revoke(context.TODO(), jobLock.LeaseId) //释放租约
	}

}
