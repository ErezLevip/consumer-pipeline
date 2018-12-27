package consumer_pipeline

import "log"

func Errors(next MessageHandler) func(*MessageContext) {
	return func(ctx *MessageContext) {
		next(ctx)
		reporter := ctx.Logger
		errors := ctx.ctx.Value("errors")
		if errors == nil {
			return
		}

		currentHandler := ctx.ctx.Value("current_handler").(string)
		for _, err := range errors.([]*ErrorMetric) {
			log.Println(err)
			reporter.Send("errors",
				1, map[string]string{
					`context`:  ctx.ctx.Value("topic").(string),
					`function`: currentHandler,
				})
		}
	}
}
