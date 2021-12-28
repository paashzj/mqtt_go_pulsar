package mqsar

import (
	"fmt"
	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/fhmq/hmq/broker"
	"os"
	"os/signal"
	"strconv"
)

type Config struct {
	MqttConfig   MqttConfig
	PulsarConfig PulsarConfig
}

type MqttConfig struct {
	Port int
}

type PulsarConfig struct {
	Host     string
	HttpPort int
	TcpPort  int
}

func RunFront(config *Config, impl Server) (err error) {
	err = Run(config, impl)
	if err != nil {
		return
	}
	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt)
	for {
		<-interrupt
		return nil
	}
}

func Run(config *Config, impl Server) (err error) {
	mqttConfig := &broker.Config{}
	mqttConfig.Port = strconv.Itoa(config.MqttConfig.Port)
	clientOptions := pulsar.ClientOptions{}
	clientOptions.URL = fmt.Sprintf("pulsar://%s:%d", config.PulsarConfig.Host, config.PulsarConfig.TcpPort)
	mqttConfig.Plugin.Bridge, err = newPulsarBridgeMq(config.MqttConfig, clientOptions, impl)
	mqttConfig.Plugin.Auth = newPulsarAuthMq(impl)
	if err != nil {
		return
	}
	newBroker, err := broker.NewBroker(mqttConfig)
	if err != nil {
		return err
	}
	newBroker.Start()
	return nil
}
