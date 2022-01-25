package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

const (
	namespace = "mqtt_pulsar"
)

func Init() {
	prometheus.MustRegister()
}
