package consumer_pipeline

import (
	"bufio"
	"context"
	"github.com/erezlevip/consumer-pipeline/acceptable_interfaces"
	"io"
	"io/ioutil"
)

type MessageContext struct{
	ctx context.Context
	Logger acceptable_interfaces.MetricsLogger
	Message io.Reader
}

func (msgCtx *MessageContext) Registry () interface{}  {
	return msgCtx.ctx.Value("registry")
}

func NewMessageContext(topic string,message io.Reader,consumerCtx context.Context,logger acceptable_interfaces.MetricsLogger) *MessageContext  {
	ctx := context.WithValue(consumerCtx,"topic",topic)
	return &MessageContext{
		Message:		message,
		ctx:ctx,
		Logger:logger,
	}
}

func (msgCtx *MessageContext) ReadMessage () ([]byte,error)  {
	r := bufio.NewReader(msgCtx.Message)
	return ioutil.ReadAll(r)
}

func Error(ctx context.Context,err ErrorMetric)  {
	errs := ctx.Value("errors")
	errs = append(errs.([]ErrorMetric),err)
}