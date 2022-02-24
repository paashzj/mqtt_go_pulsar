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

package consume

import (
	"context"
	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/eclipse/paho.mqtt.golang/packets"
	"github.com/fhmq/hmq/broker"
	"github.com/paashzj/mqtt_go_pulsar/pkg/module"
	"github.com/paashzj/mqtt_go_pulsar/pkg/service"
	"github.com/paashzj/mqtt_go_pulsar/pkg/sky"
	"github.com/sirupsen/logrus"
	v3 "skywalking.apache.org/repo/goapi/collect/language/agent/v3"
	"strings"
	"sync"
)

const (
	ConsumerClosed = "consumer closed"
)

var msgId = uint16(0)
var mutex sync.Mutex

func StartConsumeRoutine(topicKey module.MqttTopicKey, consumer pulsar.Consumer, tracer *sky.NoErrorTracer, consumerTopic string) {
	go func() {
		for {
			receiveMsg, err := consumer.Receive(context.TODO())
			if err != nil {
				if strings.Contains(err.Error(), ConsumerClosed) {
					logrus.Errorf("consumer is closed. username: %s, clientId: %s topic: %s", topicKey.Username, topicKey.ClientId, topicKey.Topic)
					return
				}
				logrus.Error("receive error is ", err)
				continue
			}
			localSpan, _, spanErr := tracer.CreateEntrySpan(context.TODO(), "consume-pulsar", func(headerKey string) (string, error) {
				return "", nil
			})
			if spanErr != nil {
				logrus.Debug("create span err", spanErr)
			} else {
				localSpan.SetSpanLayer(v3.SpanLayer_MQ)
				localSpan.SetOperationName("consume pulsar")
				localSpan.Tag("topic", consumerTopic)
			}
			mqttBroker := service.GetMqttBroker()
			publishPacket := packets.NewControlPacket(packets.Publish).(*packets.PublishPacket)
			publishPacket.TopicName = topicKey.Topic
			publishPacket.Payload = receiveMsg.Payload()
			publishPacket.Qos = broker.QosAtLeastOnce
			publishPacket.Retain = false
			publishPacket.Dup = false
			publishPacket.MessageID = getMsgId()
			mqttBroker.PublishMessage(publishPacket)
			consumer.Ack(receiveMsg)
			if spanErr == nil {
				localSpan.End()
			}
		}
	}()
}

func getMsgId() uint16 {
	mutex.Lock()
	if msgId >= 65535 {
		msgId = 0
	}
	msgId++
	mutex.Unlock()
	return msgId
}
