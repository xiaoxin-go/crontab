package master

import (
	"context"
	"crontab/owenliang/crontab/common"
	"fmt"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type LogMgr struct{
	client *mongo.Client
	logCollection *mongo.Collection
}

var (
	G_logMgr *LogMgr
)

func InitLogMgr()(err error){
	clientOptions := options.Client().ApplyURI(G_Config.MongodbUri)
	client, err := mongo.Connect(context.TODO(), clientOptions)
	if err != nil{
		return
	}
	collection := client.Database("cron").Collection("log")
	G_logMgr = &LogMgr{client: client, logCollection: collection}
	return
}

// 查看任务日志
func (logMgr *LogMgr)ListLog(name string, skip int, limit int)(logArr []*common.JobLog, err error){
	var(
		filter *common.JobLogFilter
		logSort *common.SortLogByStartTime
	)

	logArr = make([]*common.JobLog, 0)
	// 过滤条件
	filter = &common.JobLogFilter{JobName: name}
	fmt.Println("filter: ", filter)

	// 按照任务开始时间倒排
	logSort = &common.SortLogByStartTime{SortOrder: -1}

	findOptions := options.Find()
	findOptions.SetSort(logSort)
	findOptions.SetSkip(int64(skip))
	findOptions.SetLimit(int64(limit))


	cursor, err := logMgr.logCollection.Find(context.TODO(), filter, findOptions)
	if err != nil{
		return
	}
	//cursor.All(context.TODO(), &logArr)
	for cursor.Next(context.TODO()){
		fmt.Println("next: /..")
		var jobLog common.JobLog
		err := cursor.Decode(&jobLog)
		if err != nil{
			fmt.Println("日志获取转换异常")
			continue
		}
		logArr = append(logArr, &jobLog)
	}
	return
}