package consumer_pipeline

func Errors(next MessageHandler) func(*MessageContext) {
	return func(ctx *MessageContext) {
		next(ctx)
		reporter := ctx.Logger
		errors := ctx.ctx.Value("errors")
		if errors == nil {
			return
		}

		for _,err := range errors.([]*ErrorMetric){
			reporter.Send("errors",
				1, map[string]string{
					`context`:  ctx.ctx.Value("topic").(string),
					`function`: err.Function,
				})
		}
	}
}
