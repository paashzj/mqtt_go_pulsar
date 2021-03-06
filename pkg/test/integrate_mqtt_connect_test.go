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
	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/stretchr/testify/assert"
	"testing"
)

// TestMqttConnect steps:
// - create pulsar
// - create mqsar
// - create mqtt client
// - mqtt client connect
func TestMqttConnect(t *testing.T) {
	setupPulsar()
	broker, port := setupMqsar()
	defer broker.Close()
	ops := mqtt.NewClientOptions().SetUsername("username").SetClientID("foo").AddBroker(MqttConnAddr(port))
	mqttCli := mqtt.NewClient(ops)
	token := mqttCli.Connect()
	token.Wait()
	assert.True(t, mqttCli.IsConnected())
}

// TestMqttUnConnected steps:
// - create pulsar
// - create mqsar
// - create mqtt client
// - mqtt client connect
// - mqtt client check connection
func TestMqttUnConnected(t *testing.T) {
	setupPulsar()
	_, port := setupMqsar()
	ops := mqtt.NewClientOptions().SetUsername("username").SetPassword("wrong_password").SetClientID("foo").AddBroker(MqttConnAddr(port))
	mqttCli := mqtt.NewClient(ops)
	token := mqttCli.Connect()
	token.Wait()
	assert.True(t, !mqttCli.IsConnected())
}
