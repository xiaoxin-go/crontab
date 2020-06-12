package worker

import (
	"context"
	"crontab/owenliang/crontab/common"
	"go.etcd.io/etcd/clientv3"
	"net"
	"time"
)


// 注册节点到etcd: /cron/workers/ip地址
type Register struct{
	client *clientv3.Client
	kv clientv3.KV
	lease clientv3.Lease

	localIP string
}

var (
	G_register *Register
)

func getLocalIP() (ipv4 string, err error){
	// 获取所有网卡
	addrList, err := net.InterfaceAddrs()
	if err != nil{
		return
	}
	// 取第一个非localhost的网卡
	for _, addr := range addrList{
		// ipv4, ipv6, 需要对IP地址做反解
		ipNet, isIpNet := addr.(*net.IPNet)
		if isIpNet && !ipNet.IP.IsLoopback(){
			// 这个网络地址是IP地址：ipv4或ipv6，跳过IPV6
			if ipNet.IP.To4() != nil{
				ipv4 = ipNet.IP.String()
				return
			}
		}
	}
	err = common.ERR_NO_LOCAL_IP_FOUND
	return
}

// 注册到/cron/workoers/IP， 并自动续租
func(register *Register) keepOnline(){
	var (
		keepAliveChan <- chan *clientv3.LeaseKeepAliveResponse
		//putResp *clientv3.PutResponse
		keepAliveResp *clientv3.LeaseKeepAliveResponse
		cancelCtx context.Context
		cancelFunc context.CancelFunc
	)
	for {
		// 注册key
		regKey := common.JOB_WORKER_DIR + register.localIP
		cancelFunc = nil
		// 创建租约
		leaseGrantResp, err := register.lease.Grant(context.TODO(), 10)
		if err != nil{
			goto RETRY
		}

		// 自动续租
		if keepAliveChan, err = register.lease.KeepAlive(context.TODO(), leaseGrantResp.ID);err != nil{
			goto RETRY
		}

		cancelCtx, cancelFunc = context.WithCancel(context.TODO())

		// 注册到ETCD
		if _, err = register.kv.Put(cancelCtx, regKey, "", clientv3.WithLease(leaseGrantResp.ID)); err != nil{
			goto RETRY
		}

		// 处理续租应答
		for {
			select{
			case keepAliveResp = <- keepAliveChan:
				if keepAliveResp == nil{
					// 续租失败
					goto RETRY
				}
			}
		}

	RETRY:
		// 如果续租发生异常，把租约取消，重新生成租约
		time.Sleep(1 * time.Second)
		if cancelFunc != nil{
			cancelFunc()
		}
	}

}

func InitRegister()(err error){
	var(
		config clientv3.Config
		client *clientv3.Client
		kv clientv3.KV
		lease clientv3.Lease
	)
	// 初始化配置
	config = clientv3.Config{
		Endpoints: G_Config.EtcdEndpoints,
		DialTimeout: time.Duration(G_Config.EtcdDialTimeout) * time.Millisecond,
	}
	// 建立连接
	if client, err = clientv3.New(config); err != nil{
		return
	}
	// 得到kv和lease的api子集
	kv = clientv3.NewKV(client)
	lease = clientv3.NewLease(client)

	localIP, err := getLocalIP()
	if err != nil{
		return
	}

	// 赋值单例
	G_register = &Register{
		client: client,
		kv: kv,
		lease: lease,
		localIP: localIP,
	}

	// 注册
	go G_register.keepOnline()
	return
}
