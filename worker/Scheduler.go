package worker

import (
	"crontab/common"
	"fmt"
	"time"
)

//任务调度
type Scheduler struct {
	jobEventChan      chan *common.JobEvent               //etcd 任务事件
	jobPlanTable      map[string]*common.JobSchedulerPlan //任务调度计划表
	jobExecutingTable map[string]*common.JobExecuteInfo   //任务执行表
	jobResultChan     chan *common.JobExecuteResult       //任务结果队列
}

var (
	G_scheduler *Scheduler
)

//HandleJobEvent 处理任务事件
func (scheduler *Scheduler) HandleJobEvent(jobEvent *common.JobEvent) {
	var (
		jobSchedulePlan *common.JobSchedulerPlan
		jobExecuteInfo  *common.JobExecuteInfo
		jobExecuting    bool
		jobExited       bool
		err             error
	)
	switch jobEvent.EventType {
	case common.JOB_EVENT_SAVE:
		if jobSchedulePlan, err = common.BuildJobSchedulePlan(jobEvent.Job); err != nil {
			return
		}
		scheduler.jobPlanTable[jobEvent.Job.Name] = jobSchedulePlan
	case common.JOB_EVENT_DELETE:
		if jobSchedulePlan, jobExited = scheduler.jobPlanTable[jobEvent.Job.Name]; jobExited {
			delete(scheduler.jobPlanTable, jobEvent.Job.Name)
		}
	case common.JOB_EVENT_KILL:
		fmt.Println("触发killer:", jobEvent.Job.Name)
		if jobExecuteInfo, jobExecuting = scheduler.jobExecutingTable[jobEvent.Job.Name]; jobExecuting {
			jobExecuteInfo.CancelFunc() //触发command kill shell子进程
		}

	}
}

//TryStartJob 尝试执行任务
func (scheduler *Scheduler) TryStartJob(jobPlan *common.JobSchedulerPlan) {

	var (
		jobExecuteInfo *common.JobExecuteInfo
		jobExecuting   bool
	)
	//如果任务正在执行，跳过本次调度
	if jobExecuteInfo, jobExecuting = scheduler.jobExecutingTable[jobPlan.Job.Name]; jobExecuting {
		fmt.Println("尚未退出，跳过执行；", jobExecuteInfo.Job.Name)
		return
	}
	//构建执行状态信息
	jobExecuteInfo = common.BuildJobExecuteInfo(jobPlan)
	//保存执行状态
	scheduler.jobExecutingTable[jobPlan.Job.Name] = jobExecuteInfo
	//执行任务
	fmt.Println("执行任务：", jobExecuteInfo.Job.Name, jobExecuteInfo.RealTime)
	G_executor.ExecuteJob(jobExecuteInfo)

}

//TrySchedule 重新计算任务调度状态
func (scheduler *Scheduler) TrySchedule() (schedulerAfter time.Duration) {
	var (
		jobPlan  *common.JobSchedulerPlan
		now      time.Time
		nearTime *time.Time
	)

	if len(scheduler.jobPlanTable) == 0 {
		schedulerAfter = 1 * time.Second
		return
	}

	//当前时间
	now = time.Now()
	//1遍历所有任务]
	for _, jobPlan = range scheduler.jobPlanTable {
		if jobPlan.NextTime.Before(now) || jobPlan.NextTime.Equal(now) {
			//更新下次执行时间
			jobPlan.NextTime = jobPlan.Expr.Next(now)
			scheduler.TryStartJob(jobPlan)
		}
		//统计最近一个要过期的任务时间
		if nearTime == nil || jobPlan.NextTime.Before(*nearTime) {
			nearTime = &jobPlan.NextTime
		}
	}

	//下次任务调用间隔（最近要执行的任务调度时间-当前时间）
	schedulerAfter = (*nearTime).Sub(now)
	return
}

func (scheduler *Scheduler) HandleResult(jobResult *common.JobExecuteResult) {
	var (
		jobLog *common.JobLog
	)
	//删除执行状态
	delete(scheduler.jobExecutingTable, jobResult.ExecuteInfo.Job.Name)
	if jobResult.Err != common.ERR_LOCK_ALREADY_REQUIRED {
		jobLog = &common.JobLog{
			JobName:      jobResult.ExecuteInfo.Job.Name,
			Command:      jobResult.ExecuteInfo.Job.Command,
			OutPut:       string(jobResult.OutPut),
			PlanTime:     jobResult.ExecuteInfo.PlanTime.UnixNano() / 1000 / 1000,
			ScheduleTime: jobResult.ExecuteInfo.RealTime.UnixNano() / 1000 / 1000,
			StartTime:    jobResult.StartTime.UnixNano() / 1000 / 1000,
			EndTime:      jobResult.EndTime.UnixNano() / 1000 / 1000,
		}
		if jobResult.Err != nil {
			jobLog.Err = jobResult.Err.Error()
		} else {
			jobLog.Err = ""
		}
		//TODO 存储到mongodb
		G_logSink.Append(jobLog)
	}
	fmt.Println("任务执行完成：", jobResult.ExecuteInfo.Job.Name, jobResult.EndTime, string(jobResult.OutPut), jobResult.Err)
}

//scheduleLoop 调度协程
func (scheduler *Scheduler) scheduleLoop() {
	//定时任务common.Job
	var (
		jobEvent  *common.JobEvent
		jobResult *common.JobExecuteResult
	)

	//初始化
	scheduleAfter := scheduler.TrySchedule()

	//调度的延迟定时器
	scheduleTime := time.NewTimer(scheduleAfter)

	for {
		fmt.Println("\n\n===========for==========", time.Now())
		select {
		case jobEvent = <-scheduler.jobEventChan: //监听任务变化事件
			fmt.Println("===========jobEventChan====" + jobEvent.Job.Name + "======")
			//对内存中维护的任务列表做增删改查
			scheduler.HandleJobEvent(jobEvent)
		case <-scheduleTime.C: //最近的任务到期
			fmt.Println("===========<-scheduleTime.C=====" + jobEvent.Job.Name + "=====")
		case jobResult = <-scheduler.jobResultChan: //监听任务执行结果
			fmt.Println("===========jobResult====" + jobEvent.Job.Name + "======")
			scheduler.HandleResult(jobResult)
		}
		//调度一次任务
		scheduleAfter = scheduler.TrySchedule()
		//重置调度间隔
		scheduleTime.Reset(scheduleAfter)
	}

}

//PushJobEvent 推送任务变化事件
func (scheduler *Scheduler) PushJobEvent(jobEvent *common.JobEvent) {
	scheduler.jobEventChan <- jobEvent
}

func InitScheduler() (err error) {
	G_scheduler = &Scheduler{
		jobEventChan:      make(chan *common.JobEvent, 1000),
		jobPlanTable:      make(map[string]*common.JobSchedulerPlan),
		jobExecutingTable: make(map[string]*common.JobExecuteInfo),
		jobResultChan:     make(chan *common.JobExecuteResult, 1000),
	}
	//启动调度协程
	go G_scheduler.scheduleLoop()
	return
}

//PushJobResult 回传任务执行结果
func (scheduler *Scheduler) PushJobResult(jobResult *common.JobExecuteResult) {
	scheduler.jobResultChan <- jobResult
}
