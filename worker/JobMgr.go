package worker

import (
	"context"
	"crontab/common"
	"fmt"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientV3 "go.etcd.io/etcd/client/v3"
	"time"
)

//JobMgr 任务管理器
type JobMgr struct {
	client  *clientV3.Client
	kv      clientV3.KV
	lease   clientV3.Lease
	watcher clientV3.Watcher
}

var (
	G_jobMgr *JobMgr //单例
)

//监听任务变化
func (jobMgr *JobMgr) watchJobs() (err error) {
	var (
		getResp            *clientV3.GetResponse
		kvpair             *mvccpb.KeyValue
		job                *common.Job
		watchStartRevision int64
		watchChan          clientV3.WatchChan
		watchResp          clientV3.WatchResponse
		watchEvent         *clientV3.Event
		jobName            string
		jobEvent           *common.JobEvent
	)
	//1.get一下/cron/jobs/目录下的所有任务，并且获知当前集群的revision
	if getResp, err = jobMgr.kv.Get(context.TODO(), common.JOB_SAVE_DIR, clientV3.WithPrefix()); err != nil {
		return err
	}
	for _, kvpair = range getResp.Kvs {
		if job, err = common.UnPackJob(kvpair.Value); err != nil {
			continue
		}
		jobEvent = common.BuildJobEvent(common.JOB_EVENT_SAVE, job)
		fmt.Println("====jobEvent============", jobEvent)
		G_scheduler.PushJobEvent(jobEvent)
		//TODO 吧这个job同步给schedule
	}

	//2.从该revision向后监听变化事件
	go func() {
		//从get时刻开始的后续版本开始监听变化
		watchStartRevision = getResp.Header.Revision + 1
		//监听/cron/jobs/目录的后续变化
		watchChan = jobMgr.watcher.Watch(context.TODO(), common.JOB_SAVE_DIR, clientV3.WithRev(watchStartRevision), clientV3.WithPrefix())
		//处理监听事件

		for watchResp = range watchChan {
			for _, watchEvent = range watchResp.Events {
				switch watchEvent.Type {
				case mvccpb.PUT:
					if job, err = common.UnPackJob(watchEvent.Kv.Value); err != nil {
						continue
					}
					jobEvent = common.BuildJobEvent(common.JOB_EVENT_SAVE, job)
				case mvccpb.DELETE:
					jobName = common.ExtractJobName(string(watchEvent.Kv.Value))
					//构造一个删除event
					job = &common.Job{Name: jobName}
					jobEvent = common.BuildJobEvent(common.JOB_EVENT_DELETE, job)

				}
				fmt.Println(jobEvent)
				//变化推送给schedule
				G_scheduler.PushJobEvent(jobEvent)
			}
		}

	}()
	return
}

//InitJobMgr 初始化管理器
func InitJobMgr() (err error) {
	var (
		config  clientV3.Config
		client  *clientV3.Client
		kv      clientV3.KV
		lease   clientV3.Lease
		watcher clientV3.Watcher
	)
	//初始化配置
	config = clientV3.Config{
		Endpoints:   G_config.EtcdEndpoints,                                     //集群地址
		DialTimeout: time.Duration(G_config.EtcdDialTimeout) * time.Millisecond, //超时时间
	}
	fmt.Println("=====InitJobMgr=====config========", config)
	//建立连接
	if client, err = clientV3.New(config); err != nil {
		return
	}

	//获取kv和lease的api子集
	kv = clientV3.NewKV(client)
	lease = clientV3.NewLease(client)
	watcher = clientV3.NewWatcher(client)
	//赋值给单例
	G_jobMgr = &JobMgr{
		client:  client,
		kv:      kv,
		lease:   lease,
		watcher: watcher,
	}

	//启动任务监听
	if err = G_jobMgr.watchJobs(); err != nil {
		fmt.Println("watchJobs 失败:", err.Error())
		return err
	}
	//启动监听killer
	G_jobMgr.WatchKiller()

	//启动监听kill

	return
}

//CreateJobLock 创建任务执行锁
func (jobMgr *JobMgr) CreateJobLock(jobName string) (jobLock *JobLock) {
	jobLock = InitJobLock(jobName, jobMgr.kv, jobMgr.lease)
	return
}

//WatchKiller 监听强杀任务通知
func (jobMgr *JobMgr) WatchKiller() {
	var (
		watchChan  clientV3.WatchChan
		watchResp  clientV3.WatchResponse
		watchEvent *clientV3.Event
		jobEvent   *common.JobEvent
		jobName    string
		job        *common.Job
	)
	go func() {
		//监听/cron/killer/目录的后续变化
		watchChan = jobMgr.watcher.Watch(context.TODO(), common.JOB_KILLER_DIR, clientV3.WithPrefix())
		//处理监听事件

		for watchResp = range watchChan {
			for _, watchEvent = range watchResp.Events {
				switch watchEvent.Type {
				case mvccpb.PUT:
					jobName = common.ExtractKillerJobName(string(watchEvent.Kv.Key))
					job = &common.Job{Name: jobName}
					jobEvent = common.BuildJobEvent(common.JOB_EVENT_KILL, job)
					fmt.Println(jobEvent)
					//变化推送给schedule
					G_scheduler.PushJobEvent(jobEvent)
				case mvccpb.DELETE: //killer标记过期，被自动删除
				}
			}
		}

	}()
}
