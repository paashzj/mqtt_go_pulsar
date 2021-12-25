package main

import (
	"flag"
	"github.com/paashzj/mqtt_go_pulsar/pkg/mqsar"
)

var (
	mqttPort = flag.Int("mqtt_port", 1883, "mqtt listen port")
)

var (
	pulsarHost     = flag.String("pulsar_host", "localhost", "pulsar host")
	pulsarHttpPort = flag.Int("pulsar_http_port", 8080, "pulsar http port")
	pulsarTcpPort  = flag.Int("pulsar_tcp_port", 6650, "pulsar tcp port")
)

func main() {
	flag.Parse()
	config := &mqsar.Config{}
	config.MqttConfig = mqsar.MqttConfig{}
	config.MqttConfig.Port = *mqttPort
	config.PulsarConfig = mqsar.PulsarConfig{}
	config.PulsarConfig.Host = *pulsarHost
	config.PulsarConfig.HttpPort = *pulsarHttpPort
	config.PulsarConfig.TcpPort = *pulsarTcpPort
	e := &ExampleMqsarImpl{}
	err := mqsar.RunFront(config, e)
	if err != nil {
		panic(err)
	}
}
