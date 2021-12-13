package main

import (
	"flag"
	"github.com/paashzj/mqtt_go_pulsar/pkg/mqsar"
)

var pulsarHost = flag.String("pulsar_host", "localhost", "pulsar host")
var pulsarHttpPort = flag.Int("pulsar_http_port", 8080, "pulsar http port")
var pulsarTcpPort = flag.Int("pulsar_tcp_port", 6650, "pulsar tcp port")

func main() {
	flag.Parse()
	config := &mqsar.Config{}
	config.PulsarConfig = mqsar.PulsarConfig{}
	config.PulsarConfig.Host = *pulsarHost
	config.PulsarConfig.HttpPort = *pulsarHttpPort
	config.PulsarConfig.TcpPort = *pulsarTcpPort
	e := &ExampleMqsarImpl{}
	err := mqsar.Run(config, e)
	if err != nil {
		panic(err)
	}
}
