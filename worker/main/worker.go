package main

import (
	"crontab/owenliang/crontab/worker"
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
	// worker-config ./worker.json
	flag.StringVar(&confFile, "config", "./worker.json", "指定worker.json")
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
	if err := worker.InitConfig("worker.json"); err != nil{
		fmt.Println("加载配置异常", err)
		return
	}

	// 注册服务
	if err := worker.InitRegister(); err != nil{
		fmt.Println("服务注册异常", err)
		return
	}

	// 启动日志协程
	if err := worker.InitLogSink(); err != nil{
		fmt.Println("启动日志协程异常", err)
		return
	}

	// 启动执行器
	if err := worker.InitExecutor(); err != nil{
		fmt.Println("初始化执行器异常", err)
		return
	}

	// 启动调度器
	if err := worker.InitScheduler(); err != nil{
		fmt.Println("初始化调度任务异常", err)
		return
	}
	fmt.Println("初始化任务管理器----")
	// 初始化任务管理器
	if err := worker.InitJobMgr(); err != nil{
		fmt.Println("初始化任务管理器异常", err)
		return
	}

	// 不让HTTP退出
	for{
		time.Sleep(time.Second * 1)
	}
}
