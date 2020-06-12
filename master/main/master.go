package main

import (
	"crontab/owenliang/crontab/master"
	"flag"
	"fmt"
	"runtime"
	"time"
)
var (
	confFile string // 配置文件路径
)

// 解析命令行参数
func initArgs(){
	// master-config ./master.json
	flag.StringVar(&confFile, "config", "./master.json", "指定master.json")
	flag.Parse()
}

// 初始化线程数量
func initEnv(){
	runtime.GOMAXPROCS(runtime.NumCPU())
}

func main(){
	// 初始化命令行参数
	initArgs()

	// 初始化线程
	initEnv()


	// 加载配置
	if err := master.InitConfig("master.json"); err != nil{
		fmt.Println("加载配置异常", err)
		return
	}

	// 任务管理器
	if err := master.InitJobMgr(); err != nil{
		fmt.Println("etcd连接异常", err)
		return
	}

	// 初始化集群管理器
	if err := master.InitWorkerMgr(); err != nil{
		fmt.Println("worker连接异常", err)
		return
	}

	// 初始化日志管理器
	if err := master.InitLogMgr(); err != nil{
		fmt.Println("连接mongo日志异常", err)
		return
	}

	// 启动Api HTTP服务
	if err := master.InitApiServer(); err != nil{
		fmt.Println("启动服务异常", err)
		return
	}
	// 不让HTTP退出
	for{
		time.Sleep(time.Second * 1)
	}
}
