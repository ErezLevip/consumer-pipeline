package consumer_pipeline

func Errors(next MessageHandler) func(*MessageContext) {
	return func(mctx *MessageContext) {
		next(mctx)
		reporter := mctx.MetricsLogger
		if mctx.ErrorMetric == nil {
			return
		}

		function := mctx.ErrorMetric.Function
		if function == "" {
			function = mctx.Ctx.Value("current_handler").(string)
		}

		context := mctx.ErrorMetric.Context
		if context == "" {
			context = mctx.Ctx.Value("topic").(string)
		}

		reporter.Send("errors",
			1, map[string]string{
				`context`:  context,
				`function`: function,
			})

		mctx.Logger.Error(mctx.ErrorMetric.Error, "context", context, "function", function)
	}
}
