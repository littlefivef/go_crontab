package master

import (
	"context"
	"crontab/common"
	"encoding/json"
	"fmt"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientV3 "go.etcd.io/etcd/client/v3"
	"time"
)

//JobMgr 任务管理器
type JobMgr struct {
	client *clientV3.Client
	kv     clientV3.KV
	lease  clientV3.Lease
}

var (
	G_jobMgr *JobMgr //单例
)

//InitJobMgr 初始化管理器
func InitJobMgr() (err error) {
	var (
		config clientV3.Config
		client *clientV3.Client
		kv     clientV3.KV
		lease  clientV3.Lease
	)
	//初始化配置
	config = clientV3.Config{
		Endpoints:   G_config.EtcdEndpoints,                                     //集群地址
		DialTimeout: time.Duration(G_config.EtcdDialTimeout) * time.Millisecond, //超时时间
	}

	//建立连接
	if client, err = clientV3.New(config); err != nil {
		return
	}

	//获取kv和lease的api子集
	kv = clientV3.NewKV(client)
	lease = clientV3.NewLease(client)
	//赋值给单例
	G_jobMgr = &JobMgr{
		client: client,
		kv:     kv,
		lease:  lease,
	}
	return
}

//SaveJob 保存任务
func (jobMgr *JobMgr) SaveJob(job *common.Job) (oldJob *common.Job, err error) {
	//把任务保存到/cron/jobs/任务名->json
	var jobValue []byte
	jobKey := common.JOB_SAVE_DIR + job.Name
	if jobValue, err = json.Marshal(job); err != nil {
		return
	}
	//保存到etcd中
	putResp, err := jobMgr.kv.Put(context.TODO(), jobKey, string(jobValue), clientV3.WithPrevKV())
	if err != nil {
		return nil, err
	}
	//如果是更新，那么返回旧值
	if putResp.PrevKv != nil {
		//对旧值做一个反序列化
		if err = json.Unmarshal(putResp.PrevKv.Value, &oldJob); err != nil {
			err = nil //有错误也忽略
		}
	}
	return
}

func (jobMgr *JobMgr) DeleteJob(name string) (oldJob *common.Job, err error) {
	var (
		jobKey  string
		delResp *clientV3.DeleteResponse
	)
	jobKey = common.JOB_SAVE_DIR + name
	fmt.Println("===delname==--", jobKey)
	//从etcd中删除
	if delResp, err = jobMgr.kv.Delete(context.TODO(), jobKey, clientV3.WithPrevKV()); err != nil {
		return //
	}
	//返回被删除的信息
	if len(delResp.PrevKvs) != 0 {
		fmt.Println("del-value", string(delResp.PrevKvs[0].Value))
		if err = json.Unmarshal(delResp.PrevKvs[0].Value, &oldJob); err != nil {
			err = nil
		}
	}
	return
}

func (jobMgr *JobMgr) ListJob() (jobList []*common.Job, err error) {
	var (
		dirKey  string
		getResp *clientV3.GetResponse
		kvPair  *mvccpb.KeyValue
		job     *common.Job
	)
	//任务保存的目录
	dirKey = common.JOB_SAVE_DIR
	//获取目录下所有信息
	if getResp, err = jobMgr.kv.Get(context.TODO(), dirKey, clientV3.WithPrefix()); err != nil {
		return
	}
	//初始化数组空间
	jobList = make([]*common.Job, 0)
	for _, kvPair = range getResp.Kvs {
		job = &common.Job{}
		if err = json.Unmarshal(kvPair.Value, job); err != nil {
			err = nil
			continue
		}
		jobList = append(jobList, job)
	}
	return
}

func (jobMgr *JobMgr) KillJob(name string) (err error) {
	//更新一下key=/cron/killer/任务名
	var (
		killerKey string
		leaseResp *clientV3.LeaseGrantResponse
		leaseId   clientV3.LeaseID
	)
	//通知worker杀死对应任务
	killerKey = common.JOB_KILLER_DIR + name

	//让worker监听到一次put操作即可，创建一个租约让其稍后自动过期即可
	if leaseResp, err = jobMgr.lease.Grant(context.TODO(), 1); err != nil {
		return
	}
	//租约ID
	leaseId = leaseResp.ID
	//设置killer标记
	if _, err = jobMgr.kv.Put(context.TODO(), killerKey, "", clientV3.WithLease(leaseId)); err != nil {
		fmt.Println(err)
		return
	}
	return
}
