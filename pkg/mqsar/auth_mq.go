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
	"github.com/fhmq/hmq/broker"
	"github.com/fhmq/hmq/plugins/auth"
	"github.com/sirupsen/logrus"
)

type pulsarAuthMq struct {
	server   Server
	bridgeMq *PulsarBridgeMq
}

func newPulsarAuthMq(server Server, bridgeMq *PulsarBridgeMq) auth.Auth {
	return &pulsarAuthMq{
		server:   server,
		bridgeMq: bridgeMq,
	}
}

func (auth *pulsarAuthMq) CheckACL(action, clientID, username, ip, topic string) bool {
	if auth.bridgeMq.closed.Load() {
		logrus.Errorf("broker is closed. username: %s, clientId: %s", username, clientID)
		return false
	}
	switch action {
	case broker.PUB:
		_, err := auth.server.MqttProduceTopic(username, clientID, topic)
		if err != nil {
			return false
		}
	case broker.SUB:
		_, err := auth.server.MqttConsumeTopic(username, clientID, topic)
		if err != nil {
			return false
		}
	}
	return true
}

func (auth *pulsarAuthMq) CheckConnect(clientID, username, password string) bool {
	if auth.bridgeMq.closed.Load() {
		logrus.Errorf("broker is closed. username: %s, clientId: %s", username, clientID)
		return false
	}
	mqttAuth, err := auth.server.MqttAuth(username, password, clientID)
	if err != nil {
		logrus.Error("check mqtt authentication failed ", err)
		return false
	}
	return mqttAuth
}
