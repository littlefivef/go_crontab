package common

import (
	"context"
	"encoding/json"
	"github.com/gorhill/cronexpr"
	"strings"
	"time"
)

//定时任务

type Job struct {
	Name     string `json:"name"`     //任务名
	Command  string `json:"command"`  //shell命令
	CronExpr string `json:"cronExpr"` //cron表达式
}

//JobSchedulerPlan 任务调度计划
type JobSchedulerPlan struct {
	Job      *Job                 //要调度的任务信息
	Expr     *cronexpr.Expression //解析好的cronexpr表达式
	NextTime time.Time            //下次调度计划
}

//JobExecuteInfo 任务执行状态
type JobExecuteInfo struct {
	Job        *Job               //任务信息
	PlanTime   time.Time          //理论上的调度时间
	RealTime   time.Time          //实际的调度时间
	CancelCtx  context.Context    //任务command的context
	CancelFunc context.CancelFunc //用于取消command执行的cancel函数
}

//Response http接口应答
type Response struct {
	Errno int    `json:"errno"`
	Msg   string `json:"msg"`
	Data  any    `json:"data"`
}

//JobEvent 变化事件
type JobEvent struct {
	EventType int //任务类型，put delete
	Job       *Job
}

//JobExecuteResult 任务执行结果
type JobExecuteResult struct {
	ExecuteInfo *JobExecuteInfo //执行状态
	OutPut      []byte          //脚本输出
	Err         error           //脚本错误原因
	StartTime   time.Time       //启动时间
	EndTime     time.Time       //结束时间
}

type JobLog struct {
	JobName      string `json:"jobName" bson:"jobName"`            //任务名字
	Command      string `json:"command" bson:"command"`            //脚本命令
	Err          string `json:"err" bson:"err"`                    //错误原因
	OutPut       string `json:"outPut" bson:"outPut"`              //脚本输出
	PlanTime     int64  `json:"planTime" bson:"planTime"`          //计划开始时间
	ScheduleTime int64  `json:"scheduleTime"  bson:"scheduleTime"` //实际调度时间
	StartTime    int64  `json:"startTime"  bson:"startTime"`       //任务执行开始时间
	EndTime      int64  `json:"endTime"  bson:"endTime"`           //任务执行结束时间
}

//LogBatch 日志批次
type LogBatch struct {
	Logs []interface{} //多条日志
}

//JobLogFilter 任务日志过滤条件
type JobLogFilter struct {
	JobName string `bson:"jobName"`
}

//SortLogByStartTime 日志排序条件
type SortLogByStartTime struct {
	SortOrder int `bson:"startTIme"` //{"startTime":-1}
}

//BuildResponse 应答方法
func BuildResponse(errno int, msg string, data any) (resp []byte, err error) {
	//定义一个response
	response := Response{
		Errno: errno,
		Msg:   msg,
		Data:  data,
	}
	//序列化json
	resp, err = json.Marshal(response)
	return
}

//UnPackJob 反序列化job
func UnPackJob(value []byte) (ret *Job, err error) {
	ret = &Job{}
	err = json.Unmarshal(value, &ret)
	return
}

//ExtractJobName 从etcd的key中提取任务名，/cron/jobs/job10 =>job10
func ExtractJobName(jobKey string) string {
	return strings.TrimPrefix(jobKey, JOB_SAVE_DIR)
}

//ExtractKillerJobName 从etcd的key中提取任务名，/cron/killer/job10 =>job10
func ExtractKillerJobName(jobKey string) string {
	return strings.TrimPrefix(jobKey, JOB_KILLER_DIR)
}

//BuildJobEvent 任务变化事件有2种： 1）更新任务 2）删除任务
func BuildJobEvent(eventType int, job *Job) (jobEvent *JobEvent) {
	jobEvent = &JobEvent{EventType: eventType, Job: job}
	return
}

//BuildJobSchedulePlan 构造任务执行计划
func BuildJobSchedulePlan(job *Job) (jobSchedulerPlan *JobSchedulerPlan, err error) {
	var (
		expr *cronexpr.Expression
	)
	if expr, err = cronexpr.Parse(job.CronExpr); err != nil {
		return
	}

	jobSchedulerPlan = &JobSchedulerPlan{
		Job:      job,
		Expr:     expr,
		NextTime: expr.Next(time.Now()),
	}
	return
}

//BuildJobExecuteInfo 构造执行状态信息
func BuildJobExecuteInfo(jobSchedulePlan *JobSchedulerPlan) (jobExecuteInfo *JobExecuteInfo) {
	jobExecuteInfo = &JobExecuteInfo{
		Job:      jobSchedulePlan.Job,
		PlanTime: jobSchedulePlan.NextTime, //计划调度时间
		RealTime: time.Now(),               //真实调度时间

	}
	jobExecuteInfo.CancelCtx, jobExecuteInfo.CancelFunc = context.WithCancel(context.TODO())
	return
}

//ExtractWorkerIp 提取worker的IP
func ExtractWorkerIp(jobKey string) string {
	return strings.TrimPrefix(jobKey, JOB_WORKER_DIR)
}
