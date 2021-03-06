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
	"errors"
	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/fhmq/hmq/plugins/bridge"
	"github.com/paashzj/mqtt_go_pulsar/pkg/conf"
	"github.com/paashzj/mqtt_go_pulsar/pkg/consume"
	"github.com/paashzj/mqtt_go_pulsar/pkg/metrics"
	"github.com/paashzj/mqtt_go_pulsar/pkg/module"
	"github.com/paashzj/mqtt_go_pulsar/pkg/sky"
	"github.com/panjf2000/ants/v2"
	"github.com/sirupsen/logrus"
	"go.uber.org/atomic"
	v3 "skywalking.apache.org/repo/goapi/collect/language/agent/v3"
	"sync"
	"time"
)

type PulsarBridgeMq struct {
	mqttConfig         conf.MqttConfig
	pulsarConfig       conf.PulsarConfig
	pulsarClient       pulsar.Client
	server             Server
	mutex              sync.RWMutex
	pool               *ants.Pool
	sessionProducerMap map[module.MqttSessionKey][]module.MqttTopicKey
	sessionConsumerMap map[module.MqttSessionKey][]module.MqttTopicKey
	producerMap        map[module.MqttTopicKey]pulsar.Producer
	consumerMap        map[module.MqttTopicKey]pulsar.Consumer
	closed             atomic.Bool
	tracer             *sky.NoErrorTracer
}

func newPulsarBridgeMq(config conf.MqttConfig, pulsarConfig conf.PulsarConfig, options pulsar.ClientOptions, impl Server, tracer *sky.NoErrorTracer) (*PulsarBridgeMq, error) {
	client, err := pulsar.NewClient(options)
	if err != nil {
		return nil, err
	}
	size := pulsarConfig.ProduceConfig.RoutinePoolSize
	var pool *ants.Pool
	if !pulsarConfig.ProduceConfig.DisableRoutinePool {
		pool, err = ants.NewPool(size)
		if err != nil {
			logrus.Errorf("init pool faild. err: %s", err)
			return nil, err
		}
	}
	bridgeMq := &PulsarBridgeMq{mqttConfig: config, pulsarConfig: pulsarConfig, pulsarClient: client, server: impl, pool: pool, tracer: tracer}
	bridgeMq.sessionProducerMap = make(map[module.MqttSessionKey][]module.MqttTopicKey)
	bridgeMq.sessionConsumerMap = make(map[module.MqttSessionKey][]module.MqttTopicKey)
	bridgeMq.producerMap = make(map[module.MqttTopicKey]pulsar.Producer)
	bridgeMq.consumerMap = make(map[module.MqttTopicKey]pulsar.Consumer)
	return bridgeMq, nil
}

func (p *PulsarBridgeMq) PreClose() {
	p.closed.Store(true)
}

func (p *PulsarBridgeMq) Close() {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	for topicKey, producer := range p.producerMap {
		logrus.Info("producer closed ", topicKey)
		producer.Close()
	}
	for topicKey, consumer := range p.consumerMap {
		logrus.Info("consumer closed ", topicKey)
		consumer.Close()
	}
}

func (p *PulsarBridgeMq) Publish(e *bridge.Elements) (bool, error) {
	if p.closed.Load() {
		return false, errors.New("mqtt broker has been closed")
	}
	mqttSessionKey := module.MqttSessionKey{
		Username: e.Username,
		ClientId: e.ClientID,
	}
	if e.Action == bridge.Connect {
		p.handleConnect(mqttSessionKey)
	} else if e.Action == bridge.Disconnect {
		p.handleDisconnect(mqttSessionKey)
	} else if e.Action == bridge.Subscribe {
		return false, p.handleSubscribe(e, mqttSessionKey)
	} else if e.Action == bridge.Unsubscribe {
		p.handleUnsubscribe(e, mqttSessionKey)
	} else if e.Action == bridge.Publish {
		return true, p.handlePublish(e, mqttSessionKey)
	} else {
		logrus.Warn("Unsupported action ", e.Action)
	}
	return false, nil
}

func (p *PulsarBridgeMq) handleConnect(mqttSessionKey module.MqttSessionKey) {
	p.mutex.Lock()
	consumerKey := p.sessionConsumerMap[mqttSessionKey]
	if len(consumerKey) != 0 {
		for _, value := range consumerKey {
			p.closeConsumer(value)
		}
	}
	producerKey := p.sessionProducerMap[mqttSessionKey]
	if len(producerKey) != 0 {
		for _, value := range producerKey {
			p.closeProducer(value)
		}
	}
	p.sessionProducerMap[mqttSessionKey] = make([]module.MqttTopicKey, 0)
	p.sessionConsumerMap[mqttSessionKey] = make([]module.MqttTopicKey, 0)
	p.mutex.Unlock()
}

func (p *PulsarBridgeMq) handleDisconnect(mqttSessionKey module.MqttSessionKey) {
	p.mutex.Lock()
	// no topic information when close session
	p.closeSession(mqttSessionKey)
	p.mutex.Unlock()
}

func (p *PulsarBridgeMq) closeSession(mqttSessionKey module.MqttSessionKey) {
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

func (p *PulsarBridgeMq) handleSubscribe(e *bridge.Elements, mqttSessionKey module.MqttSessionKey) error {
	mqttTopicKey := module.MqttTopicKey{
		MqttSessionKey: mqttSessionKey,
		Topic:          e.Topic,
	}
	p.mutex.Lock()
	consumeTopic, err := p.server.MqttConsumeTopic(e.Username, e.ClientID, e.Topic)
	if err != nil {
		logrus.Error("get consumer topic failed ", err)
		p.mutex.Unlock()
		return err
	} else {
		consumeOptions := pulsar.ConsumerOptions{}
		consumeOptions.Type = pulsar.Shared
		consumeOptions.Topic = consumeTopic
		consumeOptions.SubscriptionName = e.Username
		consumer, err := p.pulsarClient.Subscribe(consumeOptions)
		if err != nil {
			logrus.Error("create consumer failed ", err)
			p.mutex.Unlock()
			return err
		} else {
			p.consumerMap[mqttTopicKey] = consumer
			consume.StartConsumeRoutine(mqttTopicKey, consumer, p.tracer, consumeTopic)
			p.sessionConsumerMap[mqttSessionKey] = append(p.sessionConsumerMap[mqttSessionKey], mqttTopicKey)
		}
	}
	p.mutex.Unlock()
	return nil
}

func (p *PulsarBridgeMq) handleUnsubscribe(e *bridge.Elements, mqttSessionKey module.MqttSessionKey) {
	logrus.Infof("begin to unsubscribe mqtt topic: %s", e.Topic)
	mqttTopicKey := module.MqttTopicKey{
		MqttSessionKey: mqttSessionKey,
		Topic:          e.Topic,
	}
	p.mutex.Lock()
	p.closeConsumer(mqttTopicKey)
	p.mutex.Unlock()
}

func (p *PulsarBridgeMq) handlePublish(e *bridge.Elements, mqttSessionKey module.MqttSessionKey) error {
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
			return err
		} else {
			p.mutex.Lock()
			aux = p.producerMap[mqttTopicKey]
			if aux == nil {
				producerOptions := pulsar.ProducerOptions{}
				producerOptions.DisableBatching = p.pulsarConfig.ProduceConfig.DisableBatching
				producerOptions.SendTimeout = p.pulsarConfig.ProduceConfig.SendTimeout
				producerOptions.BatchingMaxPublishDelay = p.pulsarConfig.ProduceConfig.BatchingMaxPublishDelay
				producerOptions.MaxPendingMessages = p.pulsarConfig.ProduceConfig.MaxPendingMessages
				producerOptions.DisableBlockIfQueueFull = true
				producerOptions.Topic = produceTopic
				logrus.Infof("begin to create producer. mqttTopic : %s, topic : %s", e.Topic, produceTopic)
				producer, err := p.pulsarClient.CreateProducer(producerOptions)
				if err != nil {
					logrus.Error("create produce failed ", err)
					p.mutex.Unlock()
					return err
				} else {
					p.producerMap[mqttTopicKey] = producer
					p.sessionProducerMap[mqttSessionKey] = append(p.sessionProducerMap[mqttSessionKey], mqttTopicKey)
					aux = producer
				}
			}
			p.mutex.Unlock()
		}
	}
	producerMessage := pulsar.ProducerMessage{}
	producerMessage.Payload = []byte(e.Payload)
	startTime := time.Now()
	localSpan, _, spanErr := p.tracer.CreateEntrySpan(context.TODO(), "send-pulsar", func(headerKey string) (string, error) {
		return "", nil
	})
	if spanErr != nil {
		logrus.Debug("create span err", spanErr)
	} else {
		localSpan.SetSpanLayer(v3.SpanLayer_MQ)
		localSpan.SetOperationName("send pulsar")
		localSpan.Tag("topic", aux.Topic())
	}
	if p.mqttConfig.Qos1NoWaitReply {
		task := func() {
			aux.SendAsync(context.TODO(), &producerMessage, func(id pulsar.MessageID, message *pulsar.ProducerMessage, err error) {
				if spanErr == nil {
					localSpan.End()
				}
				if err != nil {
					metrics.PulsarSendFailCount.WithLabelValues(e.Topic, aux.Topic()).Inc()
					logrus.Error("Send pulsar error ", err)
				} else {
					metrics.PulsarSendSuccessCount.WithLabelValues(e.Topic, aux.Topic()).Inc()
					logrus.Info("Send pulsar success ", id)
				}
				metrics.PulsarSendLatency.WithLabelValues(e.Topic, aux.Topic()).Observe(
					float64(time.Since(startTime).Milliseconds()))
			})
		}
		if p.pulsarConfig.ProduceConfig.DisableRoutinePool {
			go task()
		} else {
			err := p.pool.Submit(task)
			if err != nil {
				logrus.Errorf("submit send pulsar task failed. err: %s", err)
			}
		}
	} else {
		messageID, err := aux.Send(context.TODO(), &producerMessage)
		if spanErr == nil {
			localSpan.End()
		}
		if err != nil {
			metrics.PulsarSendFailCount.WithLabelValues(e.Topic, aux.Topic()).Add(1)
			logrus.Error("Send pulsar error ", err)
		} else {
			metrics.PulsarSendSuccessCount.WithLabelValues(e.Topic, aux.Topic()).Add(1)
			logrus.Info("Send pulsar success ", messageID)
		}
		metrics.PulsarSendLatency.WithLabelValues(e.Topic, aux.Topic()).Observe(
			float64(time.Since(startTime).Milliseconds()))
	}
	return nil
}

func (p *PulsarBridgeMq) closeProducer(mqttTopicKey module.MqttTopicKey) {
	producer := p.producerMap[mqttTopicKey]
	if producer != nil {
		go func() {
			logrus.Infof("begin to close producer. mqttTopic: %s, topic : %s", mqttTopicKey.Topic, producer.Topic())
			producer.Close()
		}()
	}
	p.producerMap[mqttTopicKey] = nil
}

func (p *PulsarBridgeMq) closeConsumer(mqttTopicKey module.MqttTopicKey) {
	consumer := p.consumerMap[mqttTopicKey]
	if consumer != nil {
		go func() {
			logrus.Infof("begin to close consumer. topic: %s", mqttTopicKey.Topic)
			consumer.Close()
		}()
	}
	p.consumerMap[mqttTopicKey] = nil
}
