package consumer_pipeline

import (
	"bufio"
	"context"
	"github.com/erezlevip/consumer-pipeline/acceptable_interfaces"
	"github.com/erezlevip/event-listener/types"
	"github.com/influxdata/platform/kit/errors"
	"io/ioutil"
	"reflect"
)

type MessageContext struct {
	Ctx     context.Context
	Logger  acceptable_interfaces.MetricsLogger
	Message *types.WrappedEvent
}

func (mctx *MessageContext) Registry() interface{} {
	return mctx.Ctx.Value("registry")
}

func NewMessageContext(topic string, message *types.WrappedEvent, consumerCtx context.Context, logger acceptable_interfaces.MetricsLogger) *MessageContext {
	ctx := context.WithValue(consumerCtx, "topic", topic)
	return &MessageContext{
		Message: message,
		Ctx:     ctx,
		Logger:  logger,
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

func Error(ctx context.Context, err *ErrorMetric) {
	errs := ctx.Value("errors")
	errs = append(errs.([]*ErrorMetric), err)
}