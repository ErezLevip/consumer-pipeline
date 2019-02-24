package consumer_pipeline

import (
	"bufio"
	"context"
	"github.com/erezlevip/event-listener"
	"github.com/influxdata/platform/kit/errors"
	"io/ioutil"
	"reflect"
)

type MessageContext struct {
	Ctx           context.Context
	MetricsLogger MetricsLogger
	Logger        Logger
	Message       *event_listener.WrappedEvent
	ErrorMetric   *ErrorMetric
}

func (mctx *MessageContext) Registry() interface{} {
	return mctx.Ctx.Value("registry")
}

func NewMessageContext(topic string, message *event_listener.WrappedEvent, consumerCtx context.Context, metricsLogger MetricsLogger, logger Logger) *MessageContext {
	ctx := context.WithValue(consumerCtx, "topic", topic)
	return &MessageContext{
		Message:       message,
		Ctx:           ctx,
		MetricsLogger: metricsLogger,
		Logger:        logger,
	}
}

func (mctx *MessageContext) ReadMessage() ([]byte, error) {
	if mctx.Message == nil {
		return nil, errors.New("message is nil")
	}

	r := bufio.NewReader(mctx.Message.Value)
	return ioutil.ReadAll(r)
}

func (mctx *MessageContext) RegisterCurrentHandler(handler MessageHandler) {
	mctx.Ctx = context.WithValue(mctx.Ctx, "current_handler", reflect.TypeOf(handler).Name())
}

