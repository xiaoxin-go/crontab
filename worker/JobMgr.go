package worker

import (
	"context"
	"crontab/owenliang/crontab/common"
	"fmt"
	"github.com/coreos/etcd/mvcc/mvccpb"
	"go.etcd.io/etcd/clientv3"
	"time"
)

type JobMgr struct{
	client *clientv3.Client
	kv clientv3.KV
	lease clientv3.Lease
	watcher clientv3.Watcher
}

var (
	// 单例
	G_jobMgr *JobMgr
)

// 监听任务变化
func (jobMgr *JobMgr) watchJobs()(err error){
	var jobEvent *common.JobEvent
	// 1. get一下/cron/jobs/目录下的所有任务，并且获取当前集群的revision
	getResp, err := jobMgr.kv.Get(context.TODO(), common.JOB_SAVE_DIR, clientv3.WithPrefix())
	fmt.Println("gerResp: ", getResp)
	if err != nil{
		return
	}
	for _, kvpair := range getResp.Kvs{
		// 反序列化json得到job
		job,err := common.UnPackJob(kvpair.Value)
		if err == nil{
			// 把job同步给scheduler(调度协程)
			fmt.Println(job)
			jobEvent = common.BuildJobEvent(common.JOB_EVENT_SAVE, job)
			// TODO: 是把这个job同步给scheduler(调度协程)
			G_scheduler.PushJobEvnet(jobEvent)
		}
	}

	//2. 从该revision向后监听变化事件
	go func(){	// 监听协程
		// 从get时刻的后续版本开始监听变化
		watchStartRevision := getResp.Header.Revision + 1
		// 启动监听/cron/jobs/目录的后续变化
		watchChan := jobMgr.watcher.Watch(context.TODO(), common.JOB_SAVE_DIR, clientv3.WithRev(watchStartRevision), clientv3.WithPrefix())
		// 处理监听事件
		for watchResp := range watchChan{
			for _, watchEvent := range watchResp.Events{
				switch watchEvent.Type{
				case mvccpb.PUT:
					// 任务保存事件
					// TODO: 反序列化Job, 推一个更新事件给Scheduler
					job, err := common.UnPackJob(watchEvent.Kv.Value)
					if err != nil{
						continue
					}
					// 构建一个Event事件
					jobEvent = common.BuildJobEvent(common.JOB_EVENT_SAVE, job)
				case mvccpb.DELETE:
					// 任务删除事件
					// TODO: 推一个删除事件给scheduler
					// 提取任务名
					jobName := common.ExtractJobName(string(watchEvent.Kv.Key))
					job := &common.Job{
						Name: jobName,
					}
					// 构建一个删除Event
					jobEvent = common.BuildJobEvent(common.JOB_EVENT_DELETE, job)
				}
				// 调度协程，推给scheduler
				G_scheduler.PushJobEvnet(jobEvent)
			}
		}
	}()
	return
}

// 监听强杀任务通知
func (jobMgr *JobMgr) watchKiller(){
	// 监听/cron/killer目录
	go func(){	// 启动监听协程
		// 监听/cron/killer/目录的变化
		watchChan := jobMgr.watcher.Watch(context.TODO(), common.JOB_KILLER_DIR, clientv3.WithPrefix())
		for watchResp := range watchChan{
			for _, watchEvent := range watchResp.Events{
				switch watchEvent.Type{
				case mvccpb.PUT:	// 杀死任务事件
					jobName := common.ExtractKillerName(string(watchEvent.Kv.Key))  // cron/killer/jobName, 要杀死的任务名称
					job := &common.Job{Name: jobName}
					jobEvent := common.BuildJobEvent(common.JOB_EVENT_KILL, job)	// 创建一个jobEvent
					// 把事件推给scheduler
					G_scheduler.PushJobEvnet(jobEvent)
				case mvccpb.DELETE:	// killer标记过期，被自动删除
				}
			}
		}
	}()
}

// 初始化管理器
func InitJobMgr()(err error){
	var(
		config clientv3.Config
		client *clientv3.Client
		kv clientv3.KV
		lease clientv3.Lease
		watcher clientv3.Watcher
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
	watcher = clientv3.NewWatcher(client)

	// 赋值单例
	G_jobMgr = &JobMgr{
		client: client,
		kv: kv,
		lease: lease,
		watcher: watcher,
	}

	// 启动任务监听
	G_jobMgr.watchJobs()

	// 启动监听killer
	G_jobMgr.watchKiller()
	return
}

// 创建任务执行锁
func (jobMgr *JobMgr) CreateJobLock(jobName string)(jobLock *JobLock){
	// 返回一把锁
	jobLock = InitJobLock(jobName, jobMgr.kv, jobMgr.lease)
	return
}