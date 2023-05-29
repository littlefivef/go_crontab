package master

import (
	"context"
	"crontab/common"
	clientV3 "go.etcd.io/etcd/client/v3"
	"time"
)

type WorkerMgr struct {
	client *clientV3.Client
	kv     clientV3.KV
	lease  clientV3.Lease
}

var (
	G_workerMgr *WorkerMgr
)

func (workerMgr *WorkerMgr) ListWorkers() (workerArr []string, err error) {
	//初始化
	workerArr = make([]string, 0)
	//获取目录下所有kv
	getResp, err := workerMgr.kv.Get(context.TODO(), common.JOB_WORKER_DIR, clientV3.WithPrefix())
	if err != nil {
		return
	}
	for _, kv := range getResp.Kvs {
		workerIp := common.ExtractWorkerIp(string(kv.Key))
		workerArr = append(workerArr, workerIp)
	}
	return
}

func InitWorkerMgr() (err error) {
	var (
		config clientV3.Config
		client *clientV3.Client
		kv     clientV3.KV
		lease  clientV3.Lease
	)
	config = clientV3.Config{
		Endpoints:   G_config.EtcdEndpoints,
		DialTimeout: time.Duration(G_config.EtcdDialTimeout) * time.Millisecond,
	}
	//建立连接
	if client, err = clientV3.New(config); err != nil {
		return
	}
	kv = clientV3.NewKV(client)
	lease = clientV3.NewLease(client)

	G_workerMgr = &WorkerMgr{
		client: client,
		kv:     kv,
		lease:  lease,
	}
	return
}
