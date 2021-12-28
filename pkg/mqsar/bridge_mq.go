package mqsar

import (
	"context"
	"fmt"
	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/fhmq/hmq/plugins/bridge"
	"github.com/paashzj/mqtt_go_pulsar/pkg/consume"
	"github.com/paashzj/mqtt_go_pulsar/pkg/module"
	"github.com/sirupsen/logrus"
	"sync"
)

type pulsarBridgeMq struct {
	mqttConfig                MqttConfig
	pulsarClient              pulsar.Client
	server                    Server
	mutex                     sync.RWMutex
	sessionProducerMap        map[module.MqttSessionKey][]module.MqttTopicKey
	sessionConsumerMap        map[module.MqttSessionKey][]module.MqttTopicKey
	producerMap               map[module.MqttTopicKey]pulsar.Producer
	consumerMap               map[module.MqttTopicKey]pulsar.Consumer
	consumerRoutineContextMap map[module.MqttTopicKey]*consume.RoutineContext
}

func newPulsarBridgeMq(config MqttConfig, options pulsar.ClientOptions, impl Server) (bridge.BridgeMQ, error) {
	client, err := pulsar.NewClient(options)
	if err != nil {
		return nil, err
	}
	bridgeMq := &pulsarBridgeMq{mqttConfig: config, pulsarClient: client, server: impl}
	bridgeMq.sessionProducerMap = make(map[module.MqttSessionKey][]module.MqttTopicKey)
	bridgeMq.sessionConsumerMap = make(map[module.MqttSessionKey][]module.MqttTopicKey)
	bridgeMq.producerMap = make(map[module.MqttTopicKey]pulsar.Producer)
	bridgeMq.consumerMap = make(map[module.MqttTopicKey]pulsar.Consumer)
	bridgeMq.consumerRoutineContextMap = make(map[module.MqttTopicKey]*consume.RoutineContext)
	return bridgeMq, nil
}

func (p *pulsarBridgeMq) Publish(e *bridge.Elements) error {
	if e.Username == "broker" {
		return nil
	}
	mqttSessionKey := module.MqttSessionKey{
		Username: e.Username,
		ClientId: e.ClientID,
	}
	if e.Action == bridge.Connect {
		p.mutex.Lock()
		p.sessionProducerMap[mqttSessionKey] = make([]module.MqttTopicKey, 0)
		p.sessionConsumerMap[mqttSessionKey] = make([]module.MqttTopicKey, 0)
		p.mutex.Unlock()
	} else if e.Action == bridge.Disconnect {
		p.mutex.Lock()
		p.closeSession(mqttSessionKey)
		p.mutex.Unlock()
	} else if e.Action == bridge.Subscribe {
		mqttTopicKey := module.MqttTopicKey{
			MqttSessionKey: mqttSessionKey,
			Topic:          e.Topic,
		}
		p.mutex.Lock()
		consumeTopic, err := p.server.MqttConsumeTopic(e.Username, e.ClientID, e.Topic)
		if err != nil {
			logrus.Error("get consumer topic failed ", err)
			return nil
		} else {
			consumeOptions := pulsar.ConsumerOptions{}
			consumeOptions.Type = pulsar.Shared
			consumeOptions.Topic = consumeTopic
			consumeOptions.SubscriptionName = e.Username
			consumer, err := p.pulsarClient.Subscribe(consumeOptions)
			if err != nil {
				logrus.Error("create consumer failed ", err)
				return nil
			} else {
				p.consumerMap[mqttTopicKey] = consumer
				routineContext := consume.StartConsumeRoutine(fmt.Sprintf("tcp://localhost:%d", p.mqttConfig.Port), mqttTopicKey, consumer)
				p.consumerRoutineContextMap[mqttTopicKey] = routineContext
			}
		}
		p.mutex.Unlock()
	} else if e.Action == bridge.Unsubscribe {
		mqttTopicKey := module.MqttTopicKey{
			MqttSessionKey: mqttSessionKey,
			Topic:          e.Topic,
		}
		p.mutex.Lock()
		p.closeConsumer(mqttTopicKey)
		p.mutex.Unlock()
	} else if e.Action == bridge.Publish {
		mqttTopicKey := module.MqttTopicKey{
			MqttSessionKey: mqttSessionKey,
			Topic:          e.Topic,
		}
		p.mutex.RLock()
		aux := p.producerMap[mqttTopicKey]
		p.mutex.RUnlock()
		if aux == nil {
			produceTopic, err := p.server.MqttProduceTopic(e.Username, e.ClientID, e.Topic)
			if err != nil {
				logrus.Error("get produce topic failed ", err)
				return nil
			} else {
				producerOptions := pulsar.ProducerOptions{}
				producerOptions.Topic = produceTopic
				producer, err := p.pulsarClient.CreateProducer(producerOptions)
				if err != nil {
					logrus.Error("create produce failed ", err)
					return nil
				} else {
					p.mutex.Lock()
					p.producerMap[mqttTopicKey] = producer
					p.mutex.Unlock()
				}
			}
		}
		p.mutex.RLock()
		producer := p.producerMap[mqttTopicKey]
		if producer != nil {
			producerMessage := pulsar.ProducerMessage{}
			producerMessage.Payload = []byte(e.Payload)
			go func() {
				messageID, err := producer.Send(context.TODO(), &producerMessage)
				if err != nil {
					logrus.Error("Send pulsar error ", err)
				} else {
					logrus.Info("Send pulsar success ", messageID)
				}
			}()
		}
		p.mutex.RUnlock()
	} else {
		logrus.Info("Unsupported action ", e.Action)
	}
	return nil
}

func (p *pulsarBridgeMq) closeSession(mqttSessionKey module.MqttSessionKey) {
	producers := p.sessionProducerMap[mqttSessionKey]
	for _, producer := range producers {
		p.closeProducer(producer)
	}
	p.sessionProducerMap[mqttSessionKey] = nil
	consumers := p.sessionConsumerMap[mqttSessionKey]
	for _, consumer := range consumers {
		p.closeConsumer(consumer)
	}
	p.sessionConsumerMap[mqttSessionKey] = nil
}

func (p *pulsarBridgeMq) closeProducer(mqttTopicKey module.MqttTopicKey) {
	producer := p.producerMap[mqttTopicKey]
	if producer != nil {
		producer.Close()
	}
	p.producerMap[mqttTopicKey] = nil
}

func (p *pulsarBridgeMq) closeConsumer(mqttTopicKey module.MqttTopicKey) {
	routineContext := p.consumerRoutineContextMap[mqttTopicKey]
	if routineContext != nil {
		consume.StopConsumeRoutine(routineContext)
	}
	p.consumerRoutineContextMap[mqttTopicKey] = nil
	consumer := p.consumerMap[mqttTopicKey]
	if consumer != nil {
		consumer.Close()
	}
	p.consumerMap[mqttTopicKey] = nil
}
