package mqsar

import (
	"fmt"
	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/fhmq/hmq/broker"
	"github.com/paashzj/mqtt_go_pulsar/pkg/service"
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

type Broker struct {
	mqttBroker *broker.Broker
}

func (b *Broker) DisConnClientByClientId(clientId string) {
	b.mqttBroker.DisConnClientByClientId(clientId)
}

func RunFront(config *Config, impl Server) (err error) {
	_, err = Run(config, impl)
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

func Run(config *Config, impl Server) (b *Broker, err error) {
	mqttConfig := &broker.Config{}
	mqttConfig.Port = strconv.Itoa(config.MqttConfig.Port)
	clientOptions := pulsar.ClientOptions{}
	clientOptions.URL = fmt.Sprintf("pulsar://%s:%d", config.PulsarConfig.Host, config.PulsarConfig.TcpPort)
	mqttConfig.Plugin.Bridge, err = newPulsarBridgeMq(config.MqttConfig, clientOptions, impl)
	mqttConfig.Plugin.Auth = newPulsarAuthMq(impl)
	if err != nil {
		return nil, err
	}
	newBroker, err := broker.NewBroker(mqttConfig)
	if err != nil {
		return nil, err
	}
	newBroker.Start()
	service.SetMqttBroker(newBroker)
	return &Broker{mqttBroker: newBroker}, nil
}
