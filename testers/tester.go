package main

import (
	"bytes"
	"context"
	"encoding/json"
	"github.com/erezlevip/consumer-pipeline"
	"github.com/erezlevip/event-listener"
	"log"
	"strconv"
)

func main(){

	listenerConfig := make(map[string]interface{})
	listenerConfig[event_listener.CONSUMER_RETURN_ERRORS] = strconv.FormatBool(true)
	listenerConfig[event_listener.TOPICS] = ""
	listenerConfig[event_listener.ZOOKEEPER_CONNECTION_STRING] =""
	listenerConfig[event_listener.CONSUMER_GROUP] = "group1"
	listenerConfig[event_listener.MAX_BUFFER_SIZE] = "1000"

	jsonConfig,_ := json.Marshal(listenerConfig)

	listener,err := event_listener.NewKafkaEventListener(bytes.NewReader(jsonConfig))
	if err != nil{
		log.Fatal(err)
	}

	consumer,err := consumer_pipeline.NewConsumer(context.Background(),TestLogger{},listener,nil,true)
	if err != nil{
		log.Fatal(err)
	}

	consumer.AddRouter().RegisterEndpoint("",handler,onError)

	consumer.Run()
}


func handler (ctx *consumer_pipeline.MessageContext){
	msg,err := ctx.ReadMessage()
	if err != nil{
		log.Println(err)
		return
	}

	var mp map[string]interface{}
	err  = json.Unmarshal(msg,&mp)
	if err != nil{
		log.Println(err)
		return
	}

	log.Println("this is handled")
	log.Println(mp)
}

func onError(err error,ctx context.Context)  {
	log.Println(err)
}

type TestLogger struct{}

func (TestLogger) Metric(name string, val float64, tags map[string]string)  {
	log.Println(name,val,tags)
}

func (TestLogger) Send(name string, val int64, tags map[string]string, args ...int) {
	log.Println(name,val,tags,args)
}