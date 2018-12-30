package consumer_pipeline

import (
	"fmt"
	"log"
)

type Router struct {
	handlers      map[string]MessageHandler
	errorHandlers map[string]ErrorHandler
	registry      interface{}
}

func (consumer *EventsConsumer) AddRouter() *Router {
	consumer.router = &Router{
		registry:      consumer.registry,
		handlers:      make(map[string]MessageHandler),
		errorHandlers: make(map[string]ErrorHandler),
	}
	return consumer.router
}

func (r *Router) route() MessageHandler {
	return func(mctx *MessageContext) {
		log.Println(r.handlers)
		topic := mctx.Ctx.Value("topic").(string)
		if msgHandler, exists := r.handlers[topic]; exists {
			mctx.RegisterCurrentHandler(msgHandler)
			msgHandler(mctx)
			return
		}
		log.Println("handler not found")
	}
}

func (r *Router) RegisterEndpoint(topic string, handler MessageHandler, errorHandler ErrorHandler) (*Router) {
	if _, exists := r.handlers[topic]; exists {
		log.Fatal(fmt.Sprintf("%s aready exists", topic))
	}

	r.handlers[topic] = handler
	r.errorHandlers[topic] = errorHandler
	return r
}
