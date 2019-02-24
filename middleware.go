package consumer_pipeline

type MiddlewareWrapper struct {
	chain []Middleware
}

func (ec *EventsConsumer)AddMiddlewareChain() *MiddlewareWrapper {
	if ec.chain == nil {
		ec.chain = &MiddlewareWrapper{
			chain: make([]Middleware, 0),
		}
	}
	return ec.chain
}


type Middleware func(next MessageHandler) func(ctx *MessageContext)

func (mw *MiddlewareWrapper) Add(middlewares ...Middleware) *MiddlewareWrapper {
	for _,m := range middlewares{
		mw.chain = append(mw.chain, m)
	}
	return mw
}

func (md *MiddlewareWrapper) then(handler MessageHandler) MessageHandler {
	maxIdx := len(md.chain) - 1
	builtChain := handler
	for idx := range md.chain {
		builtChain = md.chain[maxIdx-idx](builtChain)
	}
	return builtChain
}

