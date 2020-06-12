package worker

import (
	"context"
	"crontab/owenliang/crontab/common"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"time"
)

// mongodb存储日志
type LogSink struct{
	client *mongo.Client
	logCollection *mongo.Collection
	logChan chan *common.JobLog
	autoCommitChan chan *common.LogBatch
}

var (
	G_logSink *LogSink
)

// 保存日志到mongodb
func (logSink *LogSink) saveLogs(batch *common.LogBatch){
	logSink.logCollection.InsertMany(context.TODO(), batch.Logs)
}

// 日志存储协程
func (logSink *LogSink)writeLoop(){
	var (
		log *common.JobLog
		logBatch *common.LogBatch	// 当前的批次
	)
	for {
		select{
		case log = <- logSink.logChan:
			// 把这条log写到mongodb中
			// logSink.logCollection.InsertOne
			// 每次插入需要等待mongodb的一次请求往返，耗时可能因为网络慢花费比较长的时间
			if logBatch == nil{
				logBatch = &common.LogBatch{
					StartTime: time.Now().Add(time.Duration(G_Config.JobLogCommitTimeout) * time.Millisecond),
				}
			}

			// 把新的日志追加到批次中
			logBatch.Logs = append(logBatch.Logs, log)

			// 如果批次满了或者时间超过了定义的发送间隔，就一起发送
			if len(logBatch.Logs) >= G_Config.JobLogBatchSize || logBatch.StartTime.Before(time.Now()){
				logSink.saveLogs(logBatch)
				// 清空logBatch
				logBatch = nil
			}
		}
	}
}

func InitLogSink()(err error){
	var (
		client *mongo.Client
	)

	clientOptions := options.Client().ApplyURI(G_Config.MongodbUri)
	if client,err = mongo.Connect(context.TODO(), clientOptions); err != nil{
		return
	}

	// 选择db和collection
	G_logSink = &LogSink{
		client: client,
		logCollection: client.Database("cron").Collection("log"),
		logChan: make(chan *common.JobLog, 1000),
		autoCommitChan: make(chan *common.LogBatch),
	}

	go G_logSink.writeLoop()
	return
}

// 发送日志
func (logSink *LogSink) Append(jobLog *common.JobLog){
	select{
	case logSink.logChan <- jobLog:
	default:
		// 队列满了就丢弃
	}
	//logSink.logChan <- jobLog
}

