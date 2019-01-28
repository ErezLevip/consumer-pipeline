package consumer_pipeline

import (
	"context"
	"fmt"
	"github.com/erezlevip/consumer-pipeline/acceptable_interfaces"
	"github.com/erezlevip/event-listener"
	"github.com/erezlevip/event-listener/types"
	"os"
	"os/signal"
	"reflect"
)

type Consumer interface {
	Run()
	AddMiddlewareChain() *MiddlewareWrapper
	AddRouter(logger acceptable_interfaces.Logger) *Router
}

type EventsConsumer struct {
	listener                  event_listener.EventListener
	ctx                       context.Context
	metricsLogger             acceptable_interfaces.MetricsLogger
	logger                    acceptable_interfaces.Logger
	registry                  interface{}
	router                    *Router
	chain                     *MiddlewareWrapper
	chainStart                MessageHandler
	commitOnHandlerCompletion bool
}

type MessageHandler func(ctx *MessageContext)
type ErrorHandler func(ctx *MessageContext)

func NewConsumer(defaultContext context.Context, logger acceptable_interfaces.Logger, metricsLogger acceptable_interfaces.MetricsLogger, listener event_listener.EventListener, registry interface{}, commitOnHandlerCompletion bool) (Consumer, error) {
	ctx := context.WithValue(defaultContext, "registry", registry)

	return &EventsConsumer{
		ctx:                       ctx,
		listener:                  listener,
		registry:                  registry,
		metricsLogger:             metricsLogger,
		commitOnHandlerCompletion: commitOnHandlerCompletion,
		logger:                    logger,
	}, nil
}

func (e *EventsConsumer) Run() {

	e.chainStart = e.AddMiddlewareChain().Add(Logging).then(e.router.route())

	go func() {
		out, errors := e.listener.Listen()
		e.logger.Info("start listening")

		go func() {
			for t, c := range out {
				e.logger.Info(fmt.Sprintf("listening on %s", t))
				go e.handleMessages(c)
			}
		}()

		go func() {
			for t, err := range errors {
				e.logger.Info(fmt.Sprintf("handling errors on %s", t))
				go e.handleErrors(t, err)
			}
		}()
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop)

	sig := <-stop
	e.logger.Error(fmt.Sprintf("%v interrupt, sutting down", sig))
}

func (e *EventsConsumer) handleMessages(messages <-chan *types.WrappedEvent) {
	for m := range messages {
		ctx := NewMessageContext(m.Topic, m, e.ctx, e.metricsLogger, e.logger)
		e.chainStart(ctx)

		if e.commitOnHandlerCompletion {
			m.Ack()
		}
	}
}

func (e *EventsConsumer) handleErrors(topic string, errors <-chan error) {
	for err := range errors {
		ctx := NewMessageContext(topic, nil, e.ctx, e.metricsLogger, e.logger)
		ctx.ErrorMetric = &ErrorMetric{
			Error:    err,
			Function: reflect.TypeOf(e.listener.Listen).Name(),
			Context:  topic,
		}
		e.router.errorHandlers[topic](ctx)
	}
}
