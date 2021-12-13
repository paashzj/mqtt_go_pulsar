package mqsar

import (
	"context"
	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/fhmq/hmq/plugins/bridge"
	"github.com/paashzj/mqtt_go_pulsar/pkg/module"
	"github.com/sirupsen/logrus"
	"sync"
)

type pulsarBridgeMq struct {
	pulsarClient pulsar.Client
	server       Server
	mutex        sync.RWMutex
	producerMap  map[module.MqttSessionKey]pulsar.Producer
	consumerMap  map[module.MqttSessionKey]pulsar.Consumer
}

func (p *pulsarBridgeMq) Publish(e *bridge.Elements) error {
	mqttSessionKey := module.MqttSessionKey{
		Username: e.Username,
		ClientId: e.ClientID,
	}
	if e.Action == bridge.Connect {
		produceTopic, err := p.server.MqttProduceTopic(e.Username, e.ClientID)
		if err != nil {
			producerOptions := pulsar.ProducerOptions{}
			producerOptions.Topic = produceTopic
			producer, err := p.pulsarClient.CreateProducer(producerOptions)
			if err != nil {
				p.mutex.Lock()
				p.producerMap[mqttSessionKey] = producer
				p.mutex.Unlock()
			}
		}
	} else if e.Action == bridge.Disconnect {
		p.mutex.Lock()
		producer := p.producerMap[mqttSessionKey]
		if producer != nil {
			producer.Close()
		}
		p.producerMap[mqttSessionKey] = nil
		consumer := p.consumerMap[mqttSessionKey]
		if consumer != nil {
			consumer.Close()
		}
		p.mutex.Unlock()
	} else if e.Action == bridge.Subscribe {
		p.mutex.Lock()
		consumeTopic, err := p.server.MqttConsumeTopic(e.Username, e.ClientID)
		if err != nil {
			consumeOptions := pulsar.ConsumerOptions{}
			consumeOptions.Type = pulsar.Shared
			consumeOptions.Topic = consumeTopic
			consumer, err := p.pulsarClient.Subscribe(consumeOptions)
			if err != nil {
				p.mutex.Lock()
				p.consumerMap[mqttSessionKey] = consumer
				p.mutex.Unlock()
			}
		}
		p.mutex.Unlock()
	} else if e.Action == bridge.Unsubscribe {
		p.mutex.Lock()
		consumer := p.consumerMap[mqttSessionKey]
		if consumer != nil {
			consumer.Close()
		}
		p.mutex.Unlock()
	} else if e.Action == bridge.Publish {
		p.mutex.RLock()
		producer := p.producerMap[mqttSessionKey]
		if producer != nil {
			producerMessage := pulsar.ProducerMessage{}
			producerMessage.Payload = []byte(e.Payload)
			messageID, err := producer.Send(context.TODO(), &producerMessage)
			if err != nil {
				logrus.Error("Send pulsar error ", err)
			} else {
				logrus.Debug("Send pulsar success ", messageID)
			}
		}
	} else {
		logrus.Info("Unsupported action ", e.Action)
	}
	return nil
}

func newPulsarBridgeMq(options pulsar.ClientOptions, impl Server) (bridge.BridgeMQ, error) {
	client, err := pulsar.NewClient(options)
	if err != nil {
		return nil, err
	}
	return &pulsarBridgeMq{pulsarClient: client, server: impl}, nil
}
