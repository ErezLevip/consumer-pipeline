package consumer_pipeline

import (
	"context"
	"github.com/erezlevip/consumer-pipeline/acceptable_interfaces"
	"github.com/erezlevip/event-listener"
	"github.com/erezlevip/event-listener/types"
	"log"
)

type Consumer interface {
	Run(onError ErrorHandler)
	AddMiddlewareChain() *MiddlewareWrapper
	AddRouter() *Router
}

type EventsConsumer struct {
	listener   event_listener.EventListener
	ctx        context.Context
	logger     acceptable_interfaces.MetricsLogger
	registry   interface{}
	router     *Router
	chain      *MiddlewareWrapper
	chainStart MessageHandler
}

type MessageHandler func(ctx *MessageContext)
type ErrorHandler func(err error, ctx context.Context)

func NewConsumer(defaultContext context.Context,logger acceptable_interfaces.MetricsLogger, listener event_listener.EventListener, registry interface{}) (Consumer, error) {
	return &EventsConsumer{
		ctx:      context.WithValue(defaultContext, "registry", registry),
		listener: listener,
		registry: registry,
		logger:logger,
	}, nil
}

func (e *EventsConsumer) Run(onError ErrorHandler) {
	infinity := make(chan bool)

	e.chainStart = e.AddMiddlewareChain().Add(Logging).Add(Errors).then(e.router.route())

	go func() {
		out, errChannel := e.listener.Listen()
		log.Println("started listening")
		go e.handleMessages(out)
		go e.handleErrors(errChannel, onError)
	}()

	<-infinity
}

func (e *EventsConsumer) handleMessages(messages chan *types.WrappedEvent) {
	log.Println("handling messages")
	for m := range messages {
				log.Println("chan new message")
				ctx := NewMessageContext(m.Topic, m.Value, e.ctx, e.logger)
				e.chainStart(ctx)
	}
}

func (e *EventsConsumer) handleErrors(errChannel chan error, onError ErrorHandler) {
	log.Println("handling errors")
	for err := range errChannel {
		onError(err, e.ctx)
	}
}
