package master

import (
	"crontab/common"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"time"
)

//ApiServer 任务的HTTP接口
type ApiServer struct {
	httpServer *http.Server
}

var (
	//G_apiServer 单例对象
	G_apiServer *ApiServer
)

//保存任务接口
//POST job={"name":"job1","command":"echo hello","cronExpr":"* * * * *"}
func handleJobSave(w http.ResponseWriter, r *http.Request) {
	var (
		err     error
		postJob string
		job     common.Job
		oldJob  *common.Job
		bytes   []byte
	)
	//1.解析post表单
	if err = r.ParseForm(); err != nil {
		goto ERR
	}
	//取表单中的job字段
	postJob = r.PostForm.Get("job")
	fmt.Println("===postJob===", postJob)
	//反序列化job
	if err = json.Unmarshal([]byte(postJob), &job); err != nil {
		goto ERR
	}
	//4.保存到etcd
	if oldJob, err = G_jobMgr.SaveJob(&job); err != nil {
		goto ERR
	}
	//5.返回正常应答
	if bytes, err = common.BuildResponse(0, "success", oldJob); err == nil {
		_, err = w.Write(bytes)
	}
	return
ERR:
	//6.返回异常应答
	if bytes, err = common.BuildResponse(-1, err.Error(), nil); err == nil {
		_, err = w.Write(bytes)
	}
	return
}

//删除任务接口 POST /job/delete name=job1
func handleJobDelete(resp http.ResponseWriter, req *http.Request) {
	var (
		err    error
		name   string
		oldJob *common.Job
		bytes  []byte
	)
	//ParseForm 解析post表单中的数据
	if err = req.ParseForm(); err != nil {
		goto ERR
	}
	name = req.PostForm.Get("name")
	//去删除任务
	if oldJob, err = G_jobMgr.DeleteJob(name); err != nil {
		goto ERR
	}
	fmt.Println("oldJob", oldJob)
	if bytes, err = common.BuildResponse(0, "success", oldJob); err == nil {
		_, err = resp.Write(bytes)
	}
	return

ERR:
	if bytes, err = common.BuildResponse(-1, err.Error(), nil); err == nil {
		_, err = resp.Write(bytes)
	}
}

//列举所有crontab任务
func handleJobList(resp http.ResponseWriter, req *http.Request) {
	var (
		jobList []*common.Job
		err     error
		bytes   []byte
	)
	if jobList, err = G_jobMgr.ListJob(); err != nil {
		goto ERR
	}
	if bytes, err = common.BuildResponse(0, "success", jobList); err == nil {
		_, err = resp.Write(bytes)
	}
	return
ERR:
	if bytes, err = common.BuildResponse(-1, err.Error(), nil); err == nil {
		_, err = resp.Write(bytes)
	}
}

//强制杀死某个任务 post /job/kill name=job1
func handleJobKill(resp http.ResponseWriter, req *http.Request) {
	var (
		err   error
		name  string
		bytes []byte
	)
	//解析post表单
	if err = req.ParseForm(); err != nil {
		goto ERR
	}
	name = req.PostForm.Get("name")
	fmt.Println("===name===", name)
	if err = G_jobMgr.KillJob(name); err != nil {
		goto ERR
	}
	if bytes, err = common.BuildResponse(0, "success", nil); err == nil {
		_, err = resp.Write(bytes)
	}
	return
ERR:
	if bytes, err = common.BuildResponse(-1, err.Error(), nil); err == nil {
		_, err = resp.Write(bytes)
	}
}

//查询日志服务
func handleJobLog(resp http.ResponseWriter, req *http.Request) {
	var (
		err        error
		name       string
		skipParam  string
		limitParam string
		skip       int //从第几条开始
		limit      int //返回多少条
		bytes      []byte
		logArr     []*common.JobLog
	)
	if err = req.ParseForm(); err != nil {
		goto ERR
	}
	//获取请求参数/job/log?name=job10&skip=0&limit=10
	name = req.Form.Get("name")
	skipParam = req.Form.Get("skip")
	limitParam = req.Form.Get("limit")
	if skip, err = strconv.Atoi(skipParam); err != nil {
		skip = 0
	}
	if limit, err = strconv.Atoi(limitParam); err != nil {
		limit = 20
	}
	if logArr, err = G_logMgr.ListLog(name, skip, limit); err != nil {
		goto ERR
	}
	if bytes, err = common.BuildResponse(0, "success", logArr); err == nil {
		_, err = resp.Write(bytes)
	}
	return
ERR:
	if bytes, err = common.BuildResponse(-1, err.Error(), nil); err == nil {
		_, err = resp.Write(bytes)
	}
}

//获取健康worker节点列表
func handleWorkerList(resp http.ResponseWriter, req *http.Request) {
	var (
		workerArr []string
		err       error
		bytes     []byte
	)
	if workerArr, err = G_workerMgr.ListWorkers(); err != nil {
		goto ERR
	}
	if bytes, err = common.BuildResponse(0, "success", workerArr); err == nil {
		_, err = resp.Write(bytes)
	}
	return
ERR:
	if bytes, err = common.BuildResponse(-1, err.Error(), nil); err == nil {
		_, err = resp.Write(bytes)
	}
}

//InitApiServer 初始化服务
func InitApiServer() (err error) {
	//配置路由
	mux := http.NewServeMux()
	mux.HandleFunc("/job/save", handleJobSave)
	mux.HandleFunc("/job/delete", handleJobDelete)
	mux.HandleFunc("/job/list", handleJobList)
	mux.HandleFunc("/job/kill", handleJobKill)
	mux.HandleFunc("/job/log", handleJobLog)
	mux.HandleFunc("/worker/list", handleWorkerList)

	//静态文件目录
	staticDir := http.Dir(G_config.WebRoot)
	staticHandler := http.FileServer(staticDir)
	mux.Handle("/", staticHandler)

	//启动tcp协议绑定端口
	listener, err := net.Listen("tcp", ":"+strconv.Itoa(G_config.ApiPort))
	if err != nil {
		return err
	}
	//启动http服务
	httpServer := &http.Server{
		ReadTimeout:  time.Duration(G_config.ApiReadTimeout) * time.Millisecond,
		WriteTimeout: time.Duration(G_config.ApiWriteTimeout) * time.Millisecond,
		Handler:      mux,
	}
	//赋值单例
	G_apiServer = &ApiServer{httpServer: httpServer}

	go func() {
		err = httpServer.Serve(listener)
		if err != nil {
			fmt.Println(err)
		}
	}()
	return

}
