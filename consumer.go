package consumer_pipeline

import (
	"context"
	"fmt"
	"github.com/erezlevip/consumer-pipeline/acceptable_interfaces"
	"github.com/erezlevip/event-listener"
	"github.com/erezlevip/event-listener/types"
	"log"
	"reflect"
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

func NewConsumer(defaultContext context.Context, logger acceptable_interfaces.MetricsLogger, listener event_listener.EventListener, registry interface{}, commitOnHandlerCompletion bool) (Consumer, error) {
	ctx := context.WithValue(defaultContext, "registry", registry)
	ctx = context.WithValue(ctx, "errors", make([]*ErrorMetric, 0))

	return &EventsConsumer{
		ctx:                       ctx,
		listener:                  listener,
		registry:                  registry,
		logger:                    logger,
		commitOnHandlerCompletion: commitOnHandlerCompletion,
	}, nil
}

func (e *EventsConsumer) Run() {
	infinity := make(chan bool)

	e.chainStart = e.AddMiddlewareChain().Add(Logging).Add(Errors).then(e.router.route())

	go func() {
		out, errors := e.listener.Listen()
		log.Println("start listening")

		go func() {
			for t, c := range out {
				log.Println(fmt.Sprintf("listening on %s", t))
				go e.handleMessages(c)
			}
		}()

		go func() {
			for t, err := range errors {
				log.Println(fmt.Sprintf("handling errors on %s", t))
				go e.handleErrors(t, err)
			}
		}()
	}()

	<-infinity
}

func (e *EventsConsumer) handleMessages(messages <-chan *types.WrappedEvent) {
	for m := range messages {
		ctx := NewMessageContext(m.Topic, m, e.ctx, e.logger)
		e.chainStart(ctx)

		if e.commitOnHandlerCompletion {
			m.Ack()
		}
	}
}

func (e *EventsConsumer) handleErrors(topic string, errors <-chan error) {
	for err := range errors {
		Error(e.ctx,&ErrorMetric{
			Context:topic,
			Function: reflect.TypeOf(e.listener.Listen).Name(),
		})

		ctx := NewMessageContext(topic, nil, e.ctx, e.logger)
		e.router.errorHandlers[topic](err, ctx.Ctx)
	}
}
