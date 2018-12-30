package consumer_pipeline

import "log"

func Errors(next MessageHandler) func(*MessageContext) {
	return func(mctx *MessageContext) {
		next(mctx)
		reporter := mctx.Logger
		errors := mctx.Ctx.Value("errors")
		if errors == nil {
			return
		}

		for _, err := range errors.([]*ErrorMetric) {
			log.Println(err)
			function := err.Function
			if function == ""{
				function = mctx.Ctx.Value("current_handler").(string)
			}

			context := err.Context
			if context == ""{
				context = mctx.Ctx.Value("topic").(string)
			}

			reporter.Send("errors",
				1, map[string]string{
					`context`:  context,
					`function`: function,
				})
		}
	}
}
