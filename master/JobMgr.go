package master

import (
	"context"
	"crontab/owenliang/crontab/common"
	"encoding/json"
	"fmt"
	"go.etcd.io/etcd/clientv3"
	"time"
)

type JobMgr struct{
	client *clientv3.Client
	kv clientv3.KV
	lease clientv3.Lease
}

var (
	// 单例
	G_jobMgr *JobMgr
)

// 初始化管理器
func InitJobMgr()(err error){
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
	G_jobMgr = &JobMgr{
		client: client,
		kv: kv,
		lease: lease,
	}


	return
}

// 保存任务
func (jobMgr *JobMgr) SaveJob(job *common.Job)(oldJob *common.Job, err error){
	// 把任务保存到/cron/jobs/任务名 -> json
	var (
		jobKey string
		jobValue []byte
		putResp *clientv3.PutResponse
		oldJobObj common.Job
	)

	// etcd的保存key
	jobKey = common.JOB_SAVE_DIR + job.Name
	if jobValue, err = json.Marshal(job); err != nil{
		return
	}
	// 保存到etcd
	if putResp, err = jobMgr.kv.Put(context.TODO(), jobKey, string(jobValue)); err != nil{
		return
	}
	fmt.Println("putResp: ", putResp)
	// 如果是更新，那么返回旧值
	if putResp.PrevKv != nil{
		fmt.Println(putResp.PrevKv)
		// 对旧值做一个反序列化
		if err = json.Unmarshal(putResp.PrevKv.Value, &oldJobObj); err != nil{
			err = nil
			return
		}
		oldJob = &oldJobObj
	}
	return
}

// 删除任务
func(jobMgr *JobMgr) DeleteJob(name string)(oldJob *common.Job, err error){
	var (
		jobKey string
		delResp *clientv3.DeleteResponse
		oldJobObj common.Job
	)
	jobKey = common.JOB_SAVE_DIR + name
	// 删除任务
	if delResp, err = jobMgr.kv.Delete(context.TODO(), jobKey); err != nil{
		return
	}

	// 返回被删除的任务信息
	if len(delResp.PrevKvs) != 0{
		if err = json.Unmarshal(delResp.PrevKvs[0].Value, &oldJobObj); err != nil{
			err = nil
			return
		}
		oldJob = &oldJobObj
	}

	return
}

// 获取任务列表
func (jobMgr *JobMgr) ListJobs()(jobList []*common.Job, err error){
	var (
		dirKey string
		getResp *clientv3.GetResponse
		job *common.Job
	)
	dirKey = common.JOB_SAVE_DIR
	if getResp, err = jobMgr.kv.Get(context.TODO(), dirKey, clientv3.WithPrefix()); err != nil{
		return
	}


	// 初始化数组空间
	jobList = make([]*common.Job, 0)

	// 取出来数据并循环添加到jobList
	for _, kvPair := range getResp.Kvs{
		job = &common.Job{}
		if err = json.Unmarshal(kvPair.Value, job); err != nil{
			fmt.Println("err: ", err)
			continue
		}
		jobList = append(jobList, job)
	}
	return
}

// 杀死任务
func (jobMgr *JobMgr) KillJob(name string)(err error){
	// 更新一下key=/cron/killer/任务名
	var (
		killerKey string
		leaseGrantResp *clientv3.LeaseGrantResponse
		leaseId clientv3.LeaseID
	)

	// 通知worker杀死对应任务
	killerKey = common.JOB_KILLER_DIR + name

	// 让worker监听到一次put操作, 创建一个租约，让其自动过期即可
	if leaseGrantResp, err = jobMgr.lease.Grant(context.TODO(), 1); err != nil{
		return
	}

	leaseId = leaseGrantResp.ID

	// 设置killer标记
	if _, err = jobMgr.kv.Put(context.TODO(), killerKey, "", clientv3.WithLease(leaseId)); err != nil{
		return
	}

	return
}