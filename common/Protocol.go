package common

import (
	"context"
	"encoding/json"
	"github.com/gorhill/cronexpr"
	"strings"
	"time"
)

// 定时任务
type Job struct{
	Name string	`json:"name"`	// 任务名称
	Command string `json:"command"`		// 任务命令
	CronExpr string	`json:"cronExpr"`	// 任务时间
}

// 任务执行状态
type JobExecuteInfo struct{
	Job *Job	// 任务信息
	PlanTime time.Time	// 理论上的调度时间
	RealTime time.Time	// 实现上的调度时间
	CancelCtx context.Context 	// 用于取消任务Common的context
	CancelFunc context.CancelFunc	// 用于取消任务的函数
}

// 任务调度计划
type JobSchedulerPlan struct{
	Job *Job  // 要调度的任务信息
	Expr *cronexpr.Expression		// 解析好的cronexpr表达式
	NextTime time.Time
}

// HTTP接口应答
type Response struct{
	Errno int `json:"errno"`
	Msg string `json:"msg"`
	Data interface{} `json:"data"`
}

// 任务执行结果
type JobExecuteResult struct{
	ExecuteInfo *JobExecuteInfo  // 执行状态
	Output []byte  // 脚本输出
	Err error // 脚本错误原因
	StartTime time.Time // 启动时间
	EndTime time.Time // 结束时间
}

// 任务执行日志
type JobLog struct{
	JobName string `bson:"jobName"` // 任务名字
	Command string `bson:"command"` // 脚本命令
	Err string `bson:"err"`  // 错误原因
	Output string `bson:"output"`	// 脚本输出
	PlanTime int64 `bson:"planTime"` // 计划开始时间
	ScheduleTime int64 `bson:"scheduleTime"` // 实际调度时间
	StartTime int64 `bson:"startTime"` // 任务执行开始时间
	EndTime int64 `bson:"endTime"`	// 执行结果时间
}

// 日志批次
type LogBatch struct{
	Logs []interface{}	// 多条日志
	StartTime time.Time
}

// 任务日志过滤条件
type JobLogFilter struct{
	JobName string `bson:"jobName"`
}

// 任务日志排序条件
type SortLogByStartTime struct{
	SortOrder int `bson:"startTime"`
}

// 变化事件
type JobEvent struct{
	EventType int
	Job *Job
}

// 应答方法
func BuildResponse(errno int, msg string, data interface{})(resp []byte, err error){
	//1. 定义一个response
	var (
		response Response
	)
	response.Errno = errno
	response.Msg = msg
	response.Data = data

	//2. 序列化数据
	resp, err = json.Marshal(response)
	return
}

// 反序列化job
func UnPackJob(value []byte)(ret *Job, err error){
	ret = &Job{}
	err = json.Unmarshal(value, ret)
	return
}

// 从Etcd的key中提取任务名
func ExtractJobName(jobKey string)string{
	return strings.TrimPrefix(jobKey, JOB_SAVE_DIR)
}

// 从killer的key中提取任务名
func ExtractKillerName(jobKey string)string{
	return strings.TrimPrefix(jobKey, JOB_KILLER_DIR)
}

// 从worker的key中提取主机名
func ExtrackWorkerIP(worker string)string{
	return strings.TrimPrefix(JOB_WORKER_DIR, worker)
}

// 任务变化事件有2种， 1.更新任务， 2.删除任务
func BuildJobEvent(evnetType int, job *Job)(jobEvent *JobEvent){
	return &JobEvent{
		EventType: evnetType,
		Job: job,
	}
}

// 构造任务执行计划
func BuildJobSchedulePlan(job *Job)(jobSchedulerPlan *JobSchedulerPlan, err error){
	var (
		expr *cronexpr.Expression
	)

	// 解析JOB的cron表达式
	if expr, err = cronexpr.Parse(job.CronExpr); err != nil{
		return
	}

	// 生成任务调度计划对象
	jobSchedulerPlan = &JobSchedulerPlan{
		Job: job,
		Expr: expr,
		NextTime: expr.Next(time.Now()),
	}
	return
}

// 构建执行状态信息
func BuildJobExecuteInfo(jobSchedulePlan *JobSchedulerPlan)(jobExecuteInfo *JobExecuteInfo){
	jobExecuteInfo = &JobExecuteInfo{
		Job:jobSchedulePlan.Job,
		PlanTime: jobSchedulePlan.NextTime,
		RealTime: time.Now(),
	}
	jobExecuteInfo.CancelCtx, jobExecuteInfo.CancelFunc = context.WithCancel(context.TODO())
	return
}