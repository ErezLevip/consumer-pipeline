package consumer_pipeline

import (
	"fmt"
)

func Errors(next MessageHandler) func(*MessageContext) {
	return func(ctx *MessageContext) {
		next(ctx)
		reporter := ctx.Logger
		errors := ctx.ctx.Value("errors")
		if errors == nil {
			return
		}

		for _,err := range errors.([]error){
			reporter.Send("errors",
				1, map[string]string{
					`context`:  ctx.ctx.Value("topic").(string),
					`function`: fmt.Sprint(err),
				})
		}
	}
}
