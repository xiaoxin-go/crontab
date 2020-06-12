package master

import (
	"crontab/owenliang/crontab/common"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"time"
)

// 任务的HTTP接口
type ApiServer struct{
	httpServer *http.Server
}

// 杀死任务
func handleJobKill(resp http.ResponseWriter, req *http.Request){
	var (
		err error
		name string
		bytes []byte
	)
	// 解析post表单
	if err = req.ParseForm(); err != nil{
		goto ERR
	}

	// 要杀死的任务名
	name = req.PostForm.Get("name")
	if err = G_jobMgr.KillJob(name); err != nil{
		goto ERR
	}

	if bytes, err = common.BuildResponse(0, "success", nil); err == nil{
		resp.Write(bytes)
		return
	}
	ERR:
		if bytes, err = common.BuildResponse(-1, err.Error(), nil); err == nil{
			resp.Write(bytes)
		}
}

// 查看任务接口
func handleJobList(resp http.ResponseWriter, req *http.Request){
	jobList, err := G_jobMgr.ListJobs()
	if err != nil{
		if bytes, err := common.BuildResponse(-1, err.Error(), nil); err == nil{
			resp.Write(bytes)
		}
	}else{
		if bytes, err := common.BuildResponse(0, "success", jobList); err == nil{
			resp.Write(bytes)
		}
	}
}

// 删除任务接口
func handleJobDelete(resp http.ResponseWriter, req *http.Request) {
	var (
		err error
		name string
		oldJob *common.Job
		bytes []byte
	)
	if err = req.ParseForm(); err != nil{
		goto ERR
	}

	// 删除的任务名称
	name = req.PostForm.Get("name")

	// 删除任务
	if oldJob, err = G_jobMgr.DeleteJob(name);err != nil{
		goto ERR
	}

	// 正常应答
	if bytes, err = common.BuildResponse(0, "success", oldJob); err == nil{
		resp.Write(bytes)
		return
	}

ERR:
	if bytes, err = common.BuildResponse(-1, err.Error(), nil); err == nil{
		resp.Write(bytes)
	}
}
// 保存任务接口
// POST job={"name": "job1", "command": "echo hello", "conExpr": "* * * *＊"}
func handleJobSave(resp http.ResponseWriter, req *http.Request){
	var (
		err error
		postJob string
		oldJob *common.Job
		job common.Job
		bytes []byte
	)
	// 1. 解析POST表单
	if err = req.ParseForm(); err != nil{
		fmt.Println("解析表单异常", err)
		goto ERR
	}
	// 2. 取表单中的job字段
	postJob = req.PostForm.Get("job")
	fmt.Println("postJob: ", postJob)

	// 3. 反序列化job
	if err = json.Unmarshal([]byte(postJob), &job); err != nil{
		goto ERR
	}
	// 4. 保存到etcd
	if oldJob, err = G_jobMgr.SaveJob(&job); err != nil{
		fmt.Println("保存etcd异常", err)
		goto ERR
	}else{
		fmt.Println("oldJob: ", oldJob)
	}
	// 返回正常应答 {"error": 0, "msg": "", "data":{}}
	if bytes, err = common.BuildResponse(0, "success", oldJob); err == nil{
		resp.Write(bytes)
		return
	}
ERR:
	// 返回异常应答
	fmt.Println("err: ", err)
	if bytes, err = common.BuildResponse(-1, err.Error(), nil); err == nil{
		fmt.Println(bytes, err)
		resp.Write(bytes)
	}

}

// 日志接口
func handleJobLog(resp http.ResponseWriter, req *http.Request){
	var (
		err error
		name string
		page int
		pageSize int
		result []*common.JobLog
	)

	// 解析GET参数
	if err = req.ParseForm(); err != nil{
		goto ERR
	}

	// 获取请求参数
	name = req.Form.Get("name")
	if page, err = strconv.Atoi(req.Form.Get("page")); err != nil{
		page = 0
	}
	if pageSize, err = strconv.Atoi(req.Form.Get("page_size")); err != nil{
		pageSize = 20
	}
	result, err = G_logMgr.ListLog(name, page, pageSize)
	fmt.Println(result)
	if err != nil{
		goto ERR
	}
	fmt.Println("err: ", err)
	if bytes, err := common.BuildResponse(0, "success", result); err == nil{
		fmt.Println(bytes, err)
		resp.Write(bytes)
	}
	return
ERR:
	fmt.Println("err: ", err)
	if bytes, err := common.BuildResponse(-1, err.Error(), nil); err == nil{
		fmt.Println(bytes, err)
		resp.Write(bytes)
	}
}

// 获取健康主机节点
func handleWorkerList(resp http.ResponseWriter, req *http.Request){
	var (
		workerArr []string
		err error
	)
	if workerArr, err = G_workerMgr.ListWorkers(); err != nil{
		goto ERR
	}

	// 返回节点列表
	if bytes, err := common.BuildResponse(-1, "success", workerArr); err == nil{
		fmt.Println(bytes, err)
		resp.Write(bytes)
	}
ERR:
	fmt.Println("err: ", err)
	if bytes, err := common.BuildResponse(-1, err.Error(), nil); err == nil{
		fmt.Println(bytes, err)
		resp.Write(bytes)
	}
}

var(
	// 单例对象
	G_apiServer *ApiServer
)

// 初始化服务
func InitApiServer()(err error){
	// 配置路由
	mux := http.NewServeMux()
	mux.HandleFunc("/job/save", handleJobSave)
	mux.HandleFunc("/job/delete", handleJobDelete)
	mux.HandleFunc("/job/list", handleJobList)
	mux.HandleFunc("/job/kill", handleJobKill)
	mux.HandleFunc("/job/log", handleJobLog)
	mux.HandleFunc("/worker/list", handleWorkerList)

	// 静态文件目录
	staticDir := http.Dir("./webroot")
	staticHandler := http.FileServer(staticDir)
	mux.Handle("/", http.StripPrefix("/", staticHandler))

	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", G_Config.ApiPort))
	if err != nil{
		return
	}
	// 创建一个HTTP服务
	httpServer := &http.Server{
		ReadTimeout: time.Duration(G_Config.ApiReadTimeout) * time.Millisecond,
		WriteTimeout: time.Duration(G_Config.ApiWriteTimeout) * time.Millisecond,
		Handler: mux,
	}
	// 赋值单例
	G_apiServer = &ApiServer{httpServer: httpServer}

	// 启动服务端
	go httpServer.Serve(listener)
	return
}
