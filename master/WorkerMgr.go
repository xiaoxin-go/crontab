package master

import (
	"context"
	"crontab/owenliang/crontab/common"
	"go.etcd.io/etcd/clientv3"
	"time"
)

type WorkerMgr struct{
	client *clientv3.Client
	kv clientv3.KV
	lease clientv3.Lease
}

var (
	G_workerMgr *WorkerMgr
)

// 从etcd获取节点IP
func (workerMgr *WorkerMgr) ListWorkers()(workerArr []string, err error){
	var (
		getResp *clientv3.GetResponse
	)
	workerArr = make([]string, 0)
	if getResp, err = workerMgr.kv.Get(context.TODO(), common.JOB_WORKER_DIR, clientv3.WithPrefix()); err != nil{
		return
	}

	for _, kv := range getResp.Kvs{
		// kv.Key: /cron/workers/ip
		ip := common.ExtrackWorkerIP(string(kv.Key))
		workerArr = append(workerArr, ip)
	}
	return
}

func InitWorkerMgr()(err error){
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


	// 赋值单例
	G_workerMgr = &WorkerMgr{
		client: client,
		kv: kv,
		lease: lease,
	}

	// 注册
	return
}
