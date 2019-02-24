package consumer_pipeline

import (
	"context"
	"fmt"
	"log"
	"reflect"
)

type Router struct {
	handlers      map[string]MessageHandler
	errorHandlers map[string]ErrorHandler
	registry      interface{}
	logger        Logger
}

func (ec *EventsConsumer) AddRouter(logger Logger) *Router {
	ec.router = &Router{
		registry:      ec.registry,
		handlers:      make(map[string]MessageHandler),
		errorHandlers: make(map[string]ErrorHandler),
		logger:        logger,
	}
	return ec.router
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
