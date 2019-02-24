package consumer_pipeline

type MetricsLogger interface {
	Metric(name string, val float64, tags map[string]string)
	Send(name string, val int64, tags map[string]string, args ...int)
} 