package consumer_pipeline

import (
	"context"
	"fmt"
	"github.com/erezlevip/consumer-pipeline/acceptable_interfaces"
	"log"
	"reflect"
)

type Router struct {
	handlers      map[string]MessageHandler
	errorHandlers map[string]ErrorHandler
	registry      interface{}
	logger        acceptable_interfaces.Logger
}

func (consumer *EventsConsumer) AddRouter(logger acceptable_interfaces.Logger) *Router {
	consumer.router = &Router{
		registry:      consumer.registry,
		handlers:      make(map[string]MessageHandler),
		errorHandlers: make(map[string]ErrorHandler),
		logger:        logger,
	}
	return consumer.router
}

func (r *Router) route() MessageHandler {
	return func(mctx *MessageContext) {
		topic := mctx.Ctx.Value("topic").(string)
		if msgHandler, exists := r.handlers[topic]; exists {
			mctx.Ctx = context.WithValue(mctx.Ctx, "function", reflect.TypeOf(msgHandler).Name())
			mctx.RegisterCurrentHandler(msgHandler)
			msgHandler(mctx)

			if mctx.ErrorMetric != nil {
				r.logger.Error(mctx.ErrorMetric.Error, "context", mctx.ErrorMetric.Context, "function", mctx.ErrorMetric.Function)
				r.errorHandlers[topic](mctx)
			}

			return
		}
		log.Println("handler not found")
	}
}

func (r *Router) RegisterEndpoint(topic string, handler MessageHandler, errorHandler ErrorHandler) (*Router) {
	if _, exists := r.handlers[topic]; exists {
		r.logger.Fatal(fmt.Sprintf("%s aready exists", topic))
	}

	r.handlers[topic] = handler
	r.errorHandlers[topic] = errorHandler
	return r
}
