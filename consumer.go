package consumer_pipeline

import (
	"context"
	"fmt"
	"github.com/erezlevip/event-listener"
	"os"
	"os/signal"
	"reflect"
)

type Consumer interface {
	Run()
	AddMiddlewareChain() *MiddlewareWrapper
	AddRouter(logger Logger) *Router
}

type EventsConsumer struct {
	listener                  event_listener.EventListener
	ctx                       context.Context
	metricsLogger             MetricsLogger
	logger                    Logger
	registry                  interface{}
	router                    *Router
	chain                     *MiddlewareWrapper
	chainStart                MessageHandler
	commitOnHandlerCompletion bool
}

type MessageHandler func(ctx *MessageContext)
type ErrorHandler func(ctx *MessageContext)

func NewConsumer(defaultContext context.Context, logger Logger, metricsLogger MetricsLogger, listener event_listener.EventListener, registry interface{}, commitOnHandlerCompletion bool) (Consumer, error) {
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

func (ec *EventsConsumer) Run() {

	ec.chainStart = ec.AddMiddlewareChain().Add(Logging).then(ec.router.route())

	out, errors, err := ec.listener.Listen()
	if err != nil {
		ec.logger.Error(err)
		return
	}

	go func() {
		ec.logger.Info("start listening")
		go func() {
			for t, c := range out {
				ec.logger.Info(fmt.Sprintf("listening on %s", t))
				go ec.handleMessages(c)
			}
		}()

		go func() {
			for t, err := range errors {
				ec.logger.Info(fmt.Sprintf("handling errors on %s", t))
				go ec.handleErrors(t, err)
			}
		}()
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop)

	sig := <-stop
	ec.logger.Error(fmt.Sprintf("%v interrupt, sutting down", sig))
}

func (ec *EventsConsumer) handleMessages(messages <-chan *event_listener.WrappedEvent) {
	for m := range messages {
		ctx := NewMessageContext(m.Topic, m, ec.ctx, ec.metricsLogger, ec.logger)
		ec.chainStart(ctx)

		if ec.commitOnHandlerCompletion {
			err := m.Ack()
			if err != nil {
				ec.logger.Error(err)
			}
		}
	}
}

func (ec *EventsConsumer) handleErrors(topic string, errors <-chan error) {
	for err := range errors {
		ctx := NewMessageContext(topic, nil, ec.ctx, ec.metricsLogger, ec.logger)
		ctx.ErrorMetric = &ErrorMetric{
			Error:    err,
			Function: reflect.TypeOf(ec.listener.Listen).Name(),
			Context:  topic,
		}
		ec.router.errorHandlers[topic](ctx)
	}
}
