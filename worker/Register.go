package worker

import (
	"context"
	"crontab/common"
	clientV3 "go.etcd.io/etcd/client/v3"
	"net"
	"time"
)

//Register 注册节点到etcd/cron/worker/ip地址
type Register struct {
	client  *clientV3.Client
	kv      clientV3.KV
	lease   clientV3.Lease
	localIp string //本机IP
}

var (
	G_register *Register
)

//获取本机网卡IP
func getLocalIP() (ipv4 string, err error) {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return
	}
	//取第一个非localhost的网卡
	for _, addr := range addrs {
		ipNet, isIpNet := addr.(*net.IPNet)
		//如果转换成ip地址类型成功，而且地址不是loop网卡，而且其ip地址能转成ipv4类型成功
		if isIpNet && !ipNet.IP.IsLoopback() && ipNet.IP.To4() != nil {
			ipv4 = ipNet.IP.String()
			return
		}
	}
	err = common.ERR_NO_LOCAL_IP_FOUND
	return
}

//注册到/cron/worker/ 并自动续租
func (register *Register) keepOnline() {
	var (
		regKey        string
		leaseResp     *clientV3.LeaseGrantResponse
		err           error
		keepAliveChan <-chan *clientV3.LeaseKeepAliveResponse
		keepAliveResp *clientV3.LeaseKeepAliveResponse
		cancelCtx     context.Context
		cancelFunc    context.CancelFunc
	)
	//注册路径
	regKey = common.JOB_WORKER_DIR + register.localIp

	for {
		cancelFunc = nil
		//创建租约
		if leaseResp, err = register.lease.Grant(context.TODO(), 10); err != nil {
			goto RETRY
		}
		cancelCtx, cancelFunc = context.WithCancel(context.TODO())
		//注册到etcd

		//自动续租
		if keepAliveChan, err = register.lease.KeepAlive(cancelCtx, leaseResp.ID); err != nil {
			goto RETRY
		}

		if _, err = register.kv.Put(cancelCtx, regKey, "", clientV3.WithLease(leaseResp.ID)); err != nil {
			goto RETRY
		}

		//处理续租应答
		for {
			select {
			case keepAliveResp = <-keepAliveChan:
				if keepAliveResp == nil { //续租失败
					goto RETRY
				}
			}
		}
	RETRY:
		time.Sleep(1 * time.Second)
		if cancelFunc != nil {
			cancelFunc()
		}
	}

}

func InitRegister() (err error) {
	var (
		config  clientV3.Config
		client  *clientV3.Client
		kv      clientV3.KV
		lease   clientV3.Lease
		localIp string
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
	if localIp, err = getLocalIP(); err != nil {
		return
	}

	kv = clientV3.NewKV(client)
	lease = clientV3.NewLease(client)
	G_register = &Register{
		client:  client,
		kv:      kv,
		lease:   lease,
		localIp: localIp,
	}

	//启动注册服务
	go G_register.keepOnline()

	return
}
