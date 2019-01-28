package consumer_pipeline

import (
	"time"
)

func Logging(next MessageHandler) func(*MessageContext) {
	return func(ctx *MessageContext) {
		startTime := time.Now()
		reporter := ctx.MetricsLogger
		topic := ctx.Ctx.Value("topic").(string)
		reporter.Send("topic", 1, map[string]string{`name`: topic, `state`: `receive`})
		next(ctx)
		reporter.Send("handlers", 1, map[string]string{`name`: topic, `state`: `response`})
		reporter.Metric("handlers.response_time", time.Now().Sub(startTime).Seconds(), map[string]string{`name`: topic, `state`: `response`})
	}
}
