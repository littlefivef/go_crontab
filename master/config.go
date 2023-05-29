package master

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
)

//Config 程序配置
type Config struct {
	ApiPort               int      `json:"apiPort"`
	ApiReadTimeout        int      `json:"apiReadTimeout"`
	ApiWriteTimeout       int      `json:"apiWriteTimeout"`
	EtcdEndpoints         []string `json:"etcdEndpoints"`
	EtcdDialTimeout       int      `json:"etcdDialTimeout"`
	WebRoot               string   `json:"webRoot"`
	MongodbUri            string   `json:"mongodbUri"`
	MongodbConnectTimeout int      `json:"mongodbConnectTimeout"`
}

//单例
var (
	G_config *Config
)

//InitConfig 加载配置
func InitConfig(filename string) (err error) {
	//1.读取配置文件
	var (
		content []byte
		conf    Config
	)
	content, err = ioutil.ReadFile(filename)
	if err != nil {
		return err
	}
	//2.json反序列化
	err = json.Unmarshal(content, &conf)
	if err != nil {
		return err
	}
	//3.赋值单例
	G_config = &conf
	fmt.Println(G_config)
	return
}
