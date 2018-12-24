package consumer_pipeline

import (
	"bufio"
	"context"
	"github.com/erezlevip/consumer-pipeline/acceptable_interfaces"
	"io"
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
	var buff = make([]byte,4)
	message := make([]byte,0)
	for {
		n, err := r.Read(buff)
		if err != nil {
			if err == io.EOF {
				message = append(message,buff[:n]...)
				break
			}

		}
		message = append(message,buff...)
	}
	return message,nil
}