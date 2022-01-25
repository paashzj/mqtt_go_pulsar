package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	PulsarSendSuccessCount = promauto.NewCounter(prometheus.CounterOpts{
		Name: prometheus.BuildFQName(namespace, "produce", "pulsar_send_success_total"),
	})
	PulsarSendFailCount = promauto.NewCounter(prometheus.CounterOpts{
		Name: prometheus.BuildFQName(namespace, "produce", "pulsar_send_fail_total"),
	})
	PulsarSendLatency = promauto.NewSummary(prometheus.SummaryOpts{
		Name:       prometheus.BuildFQName(namespace, "produce", "pulsar_send_latency_ms"),
		Objectives: map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.99: 0.001},
	})
)
