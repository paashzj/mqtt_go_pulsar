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

package test

import (
	"context"
	"github.com/apache/pulsar-client-go/pulsar"
	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestMqttBrokerDisconnect(t *testing.T) {
	setupPulsar()
	broker, port := setupMqsar()
	defer broker.Close()
	ops := mqtt.NewClientOptions().SetUsername("username").SetClientID("foo").SetAutoReconnect(false).AddBroker(MqttConnAddr(port))
	mqttCli := mqtt.NewClient(ops)
	token := mqttCli.Connect()
	token.Wait()
	assert.True(t, mqttCli.IsConnected())
	broker.DisConnClientByClientId("foo")
	time.Sleep(5 * time.Second)
	assert.False(t, mqttCli.IsConnected())
}

// TestMqttBrokerDisconnectWithProducer test disconnect
func TestMqttBrokerDisconnectWithProducer(t *testing.T) {
	setupPulsar()
	broker, port := setupMqsar()
	pulsarClient, err := pulsar.NewClient(pulsar.ClientOptions{
		URL: "pulsar://localhost:6650",
	})
	if err != nil {
		panic(err)
	}
	mqttTopic := "mqtt-topic-test-disconnect"
	pulsarTopic := mqttProduceTopic(mqttTopic)
	ops := mqtt.NewClientOptions().SetWriteTimeout(10 * time.Second).SetUsername("username").SetClientID("foo").AddBroker(MqttConnAddr(port))
	mqttCli := mqtt.NewClient(ops)
	token := mqttCli.Connect()
	token.Wait()
	consumer, err := pulsarClient.Subscribe(pulsar.ConsumerOptions{
		Topic:            pulsarTopic,
		SubscriptionName: "mqtt-produce-test",
	})
	if err != nil {
		panic(err)
	}
	token = mqttCli.Publish(mqttTopic, 1, true, "mqtt-msg")
	broker.Close()
	token.WaitTimeout(5 * time.Second)
	logrus.Info("token err is: ", token.Error())
	ctx, cancel := context.WithTimeout(context.TODO(), 5*time.Second)
	defer cancel()
	message, err := consumer.Receive(ctx)
	assert.NotNil(t, err)
	assert.Nil(t, message)
}
