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
	"fmt"
)

type MqsarImpl struct {
}

func (m MqsarImpl) MqttAuth(username, password, clientId string) (bool, error) {
	if password == "wrong_password" {
		return false, nil
	}
	return true, nil
}

func (m MqsarImpl) MqttProduceTopic(username, clientId, topic string) (string, error) {
	if username == "username" && topic == "wrong-mqtt-topic" {
		return "", fmt.Errorf("Pub Topics Auth failed ")
	}
	return mqttProduceTopic(topic), nil
}

func mqttProduceTopic(topic string) string {
	return fmt.Sprintf("persistent://public/default/%s", topic)
}

func (m MqsarImpl) MqttConsumeTopic(username, clientId, topic string) (string, error) {
	if username == "username" && topic == "wrong-mqtt-topic" {
		return "", fmt.Errorf("Sub Topics Auth failed ")
	}
	return mqttConsumeTopic(topic), nil
}

func mqttConsumeTopic(topic string) string {
	return fmt.Sprintf("persistent://public/default/%s", topic)
}
