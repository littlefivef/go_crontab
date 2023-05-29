package worker

import (
	"context"
	"crontab/common"
	"fmt"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"log"
	"time"
)

type LogSink struct {
	client         *mongo.Client
	logCollection  *mongo.Collection
	logChan        chan *common.JobLog
	autoCommitChan chan *common.LogBatch
}

var (
	G_logSink *LogSink
)

//批次写入日志
func (logSink *LogSink) saveLogs(batch *common.LogBatch) {
	result, err := logSink.logCollection.InsertMany(context.TODO(), batch.Logs)

	if err != nil {
		fmt.Println("日志插入失败", err)
	}

	for _, docId := range result.InsertedIDs {
		fmt.Println("插入日志的ID为", docId)
	}
}

//日志存储处理协程
func (logSink *LogSink) writeLoop() {
	var (
		log          *common.JobLog
		logBatch     *common.LogBatch //当前批次
		commitTimer  *time.Timer
		timeOutBatch *common.LogBatch //超时批次
	)

	for {
		select {
		case log = <-logSink.logChan:
			fmt.Println("写log")
			if logBatch == nil {
				logBatch = &common.LogBatch{}
				commitTimer = time.AfterFunc(time.Duration(G_config.JobLogCommitTimeout)*time.Millisecond,
					func(batch *common.LogBatch) func() {
						return func() {
							logSink.autoCommitChan <- batch
						}
					}(logBatch))
				fmt.Println(commitTimer)
			}
			//把新的日志追加到批次中
			logBatch.Logs = append(logBatch.Logs, log)

			if len(logBatch.Logs) >= G_config.JobLogBatchSize {
				logSink.saveLogs(logBatch)
				//清空batch
				logBatch = nil
				//去掉定时器
				commitTimer.Stop()
			}
		case timeOutBatch = <-logSink.autoCommitChan: //过期批次
			//判断过期批次是否仍旧是当前批次
			if timeOutBatch != logBatch {
				continue
			}

			logSink.saveLogs(timeOutBatch)
			//清空batch
			logBatch = nil

		}
	}
}

func InitLogSink() (err error) {
	//1.建立链接
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	client, err := mongo.Connect(ctx,
		options.Client().ApplyURI(G_config.MongodbUri),
		options.Client().SetConnectTimeout(time.Duration(G_config.MongodbConnectTimeout)*time.Millisecond),
	)
	if err != nil {
		log.Fatal(err)
	}
	//2.选择DB和collection
	G_logSink = &LogSink{
		client:         client,
		logCollection:  client.Database("cron").Collection("log"),
		logChan:        make(chan *common.JobLog, 1000),
		autoCommitChan: make(chan *common.LogBatch, 1000),
	}
	//启动mongodb处理协程
	go G_logSink.writeLoop()
	return
}

//Append 发送日志
func (logSink *LogSink) Append(jobLock *common.JobLog) {
	select {
	case logSink.logChan <- jobLock:
	default:
		//队列满了就丢弃
	}

}
