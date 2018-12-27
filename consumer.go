package consumer_pipeline

import (
	"context"
	"fmt"
	"github.com/erezlevip/consumer-pipeline/acceptable_interfaces"
	"github.com/erezlevip/event-listener"
	"github.com/erezlevip/event-listener/types"
	"log"
)

type Consumer interface {
	Run()
	AddMiddlewareChain() *MiddlewareWrapper
	AddRouter() *Router
}

type EventsConsumer struct {
	listener                  event_listener.EventListener
	ctx                       context.Context
	logger                    acceptable_interfaces.MetricsLogger
	registry                  interface{}
	router                    *Router
	chain                     *MiddlewareWrapper
	chainStart                MessageHandler
	commitOnHandlerCompletion bool
}

type MessageHandler func(ctx *MessageContext)
type ErrorHandler func(err error, ctx context.Context)

func NewConsumer(defaultContext context.Context, logger acceptable_interfaces.MetricsLogger, listener event_listener.EventListener, registry interface{},commitOnHandlerCompletion bool) (Consumer, error) {
	return &EventsConsumer{
		ctx:      context.WithValue(defaultContext, "registry", registry),
		listener: listener,
		registry: registry,
		logger:   logger,
		commitOnHandlerCompletion:commitOnHandlerCompletion,
	}, nil
}

func (e *EventsConsumer) Run() {
	infinity := make(chan bool)

	e.chainStart = e.AddMiddlewareChain().Add(Logging).Add(Errors).then(e.router.route())

	go func() {
		out, errors := e.listener.Listen()
		log.Println("started listening")

		go func() {
			for t, c := range out {
				log.Println(fmt.Sprintf("message on on %s", t))
				e.handleMessages(c)
			}
		}()

		go func() {
			for t, err := range errors {
				log.Println(fmt.Sprintf("error on %s", t))
				e.handleErrors(t, err)
			}
		}()
	}()

	<-infinity
}

func (e *EventsConsumer) handleMessages(messages <-chan *types.WrappedEvent) {
	log.Println("chan new message")
	for m := range messages {
		log.Println("chan new message")
		ctx := NewMessageContext(m.Topic, m.Value, e.ctx, e.logger)
		e.chainStart(ctx)

		if e.commitOnHandlerCompletion{
			m.Ack()
		}
	}
}

func (e *EventsConsumer) handleErrors(topic string, errors <-chan error) {
	log.Println("handling errors")
	for err := range errors {
		ctx := NewMessageContext(topic, nil, e.ctx, e.logger)
		e.router.errorHandlers[topic](err, ctx.ctx)
	}
}
