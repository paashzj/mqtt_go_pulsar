// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package mqsar

import (
	"context"
	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/fhmq/hmq/plugins/bridge"
	"github.com/paashzj/mqtt_go_pulsar/pkg/consume"
	"github.com/paashzj/mqtt_go_pulsar/pkg/metrics"
	"github.com/paashzj/mqtt_go_pulsar/pkg/module"
	"github.com/panjf2000/ants/v2"
	"github.com/sirupsen/logrus"
	"sync"
	"time"
)

type pulsarBridgeMq struct {
	mqttConfig                MqttConfig
	pulsarClient              pulsar.Client
	server                    Server
	mutex                     sync.RWMutex
	pool                      *ants.Pool
	sessionProducerMap        map[module.MqttSessionKey][]module.MqttTopicKey
	sessionConsumerMap        map[module.MqttSessionKey][]module.MqttTopicKey
	producerMap               map[module.MqttTopicKey]pulsar.Producer
	consumerMap               map[module.MqttTopicKey]pulsar.Consumer
	consumerRoutineContextMap map[module.MqttTopicKey]*consume.RoutineContext
}

func newPulsarBridgeMq(config MqttConfig, options pulsar.ClientOptions, impl Server, pool *ants.Pool) (bridge.BridgeMQ, error) {
	client, err := pulsar.NewClient(options)
	if err != nil {
		return nil, err
	}
	bridgeMq := &pulsarBridgeMq{mqttConfig: config, pulsarClient: client, server: impl, pool: pool}
	bridgeMq.sessionProducerMap = make(map[module.MqttSessionKey][]module.MqttTopicKey)
	bridgeMq.sessionConsumerMap = make(map[module.MqttSessionKey][]module.MqttTopicKey)
	bridgeMq.producerMap = make(map[module.MqttTopicKey]pulsar.Producer)
	bridgeMq.consumerMap = make(map[module.MqttTopicKey]pulsar.Consumer)
	bridgeMq.consumerRoutineContextMap = make(map[module.MqttTopicKey]*consume.RoutineContext)
	return bridgeMq, nil
}

func (p *pulsarBridgeMq) Publish(e *bridge.Elements) error {
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
		// no topic information when close session
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
				routineContext := consume.StartConsumeRoutine(mqttTopicKey, consumer)
				p.consumerRoutineContextMap[mqttTopicKey] = routineContext
				p.sessionConsumerMap[mqttSessionKey] = append(p.sessionConsumerMap[mqttSessionKey], mqttTopicKey)
			}
		}
		p.mutex.Unlock()
	} else if e.Action == bridge.Unsubscribe {
		logrus.Infof("begin to unsubscribe mqtt topic: %s", e.Topic)
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
				producerOptions.DisableBatching = p.mqttConfig.DisableBatching
				producerOptions.SendTimeout = p.mqttConfig.SendTimeout
				producerOptions.DisableBlockIfQueueFull = true
				producerOptions.Topic = produceTopic
				logrus.Infof("begin to create producer. mqttTopic : %s, topic : %s", e.Topic, produceTopic)
				producer, err := p.pulsarClient.CreateProducer(producerOptions)
				if err != nil {
					logrus.Error("create produce failed ", err)
					return nil
				} else {
					p.mutex.Lock()
					p.producerMap[mqttTopicKey] = producer
					p.sessionProducerMap[mqttSessionKey] = append(p.sessionProducerMap[mqttSessionKey], mqttTopicKey)
					p.mutex.Unlock()
					aux = producer
				}

			}
		}
		producerMessage := pulsar.ProducerMessage{}
		producerMessage.Payload = []byte(e.Payload)
		startTime := time.Now()
		if p.mqttConfig.Qos1NoWaitReply {
			err := p.pool.Submit(func() {
				aux.SendAsync(context.TODO(), &producerMessage, func(id pulsar.MessageID, message *pulsar.ProducerMessage, err error) {
					if err != nil {
						metrics.PulsarSendFailCount.Add(1)
						logrus.Error("Send pulsar error ", err)
					} else {
						metrics.PulsarSendSuccessCount.Add(1)
						logrus.Info("Send pulsar success ", id)
					}
					metrics.PulsarSendLatency.Observe(float64(time.Now().Sub(startTime).Milliseconds()))
				})
			})
			if err != nil {
				logrus.Errorf("submit send pulsar task failed. err: %s", err)
			}
		} else {
			messageID, err := aux.Send(context.TODO(), &producerMessage)
			if err != nil {
				metrics.PulsarSendFailCount.Add(1)
				logrus.Error("Send pulsar error ", err)
			} else {
				metrics.PulsarSendSuccessCount.Add(1)
				logrus.Info("Send pulsar success ", messageID)
			}
			metrics.PulsarSendLatency.Observe(float64(time.Now().Sub(startTime).Milliseconds()))
		}
	} else {
		logrus.Info("Unsupported action ", e.Action)
	}
	return nil
}

func (p *pulsarBridgeMq) closeSession(mqttSessionKey module.MqttSessionKey) {
	logrus.Infof("begin to close mqtt session. user: %s", mqttSessionKey.Username)
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
		go func() {
			logrus.Infof("begin to close producer. mqttTopic: %s, topic : %s", mqttTopicKey.Topic, producer.Topic())
			producer.Close()
		}()
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
		go func() {
			logrus.Infof("begin to close consumer. topic: %s", mqttTopicKey.Topic)
			consumer.Close()
		}()
	}
	p.consumerMap[mqttTopicKey] = nil
}
