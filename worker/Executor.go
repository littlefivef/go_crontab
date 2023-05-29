package worker

import (
	"crontab/common"
	"math/rand"
	"os/exec"
	"time"
)

//Executor 任务执行器
type Executor struct {
}

var (
	G_executor *Executor
)

//ExecuteJob 执行一个任务
func (executor *Executor) ExecuteJob(info *common.JobExecuteInfo) {

	go func() {

		var (
			cmd     *exec.Cmd
			err     error
			outPut  []byte
			result  *common.JobExecuteResult
			jobLock *JobLock
		)
		result = &common.JobExecuteResult{
			ExecuteInfo: info,
			OutPut:      make([]byte, 0),
		}
		//记录开始时间
		result.StartTime = time.Now()

		//初始化分布式锁
		jobLock = G_jobMgr.CreateJobLock(info.Job.Name)

		//上锁
		//随机睡眠（0-1秒）
		time.Sleep(time.Duration(rand.Intn(1000)) * time.Millisecond)
		err = jobLock.TryLock()
		defer jobLock.Unlock()

		if err != nil {
			result.Err = err
			result.EndTime = time.Now()
		} else {
			//上锁成功后，重置任务开始时间
			result.StartTime = time.Now()
			//执行shell命令
			cmd = exec.CommandContext(info.CancelCtx, "/bin/bash", "-c", info.Job.Command)
			//执行并捕获输出
			outPut, err = cmd.CombinedOutput()
			result.OutPut = outPut
			result.Err = err
			result.EndTime = time.Now()
		}

		//任务执行完毕之后，把执行结果返回给scheduler，scheduler将任务重executingTable表中删除，以便下次继续执行
		G_scheduler.PushJobResult(result)

	}()
}

func InitExecutor() (err error) {
	G_executor = &Executor{}
	return
}
