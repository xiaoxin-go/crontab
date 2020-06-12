package worker

import (
	"encoding/json"
	"io/ioutil"
)

var (
	G_Config *Config
)

type Config struct{
	EtcdEndpoints []string `json:"etcdEndpoints"`
	EtcdDialTimeout int `json:"etcdDialT imeout"`
	MongodbUri string `json:"mongodbUri"`
	MongodbDialTimeout int `json:"mongodbDialTimeout"`
	JobLogBatchSize int `json:"jobLogBatchSize"`
	JobLogCommitTimeout int `json:"jobLogCommitTimeout"`
}

// 加载配置
func InitConfig(filename string)(err error){
	var (
		content []byte
		conf Config
	)
	// 1. 把配置文件读进来
	if content, err = ioutil.ReadFile(filename);err != nil{
		return
	}

	// 2. json反序列化
	if err= json.Unmarshal(content, &conf); err != nil{
		return
	}

	// 3. 赋值单例
	G_Config = &conf
	return
}
