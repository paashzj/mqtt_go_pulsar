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

import "github.com/paashzj/mqtt_go_pulsar/pkg/mqsar"

func setupMqsar() (*mqsar.Broker, int) {
	port, err := AcquireUnusedPort()
	if err != nil {
		panic(err)
	}
	broker, err := setupMqsarInternal(port)
	if err != nil {
		panic(err)
	}
	return broker, port
}

func setupMqsarInternal(port int) (*mqsar.Broker, error) {
	config := &mqsar.Config{}
	config.MqttConfig = mqsar.MqttConfig{}
	config.MqttConfig.Port = port
	config.PulsarConfig = mqsar.PulsarConfig{}
	config.PulsarConfig.Host = "localhost"
	config.PulsarConfig.HttpPort = 8080
	config.PulsarConfig.TcpPort = 6650
	mqsarImpl := &MqsarImpl{}
	return mqsar.Run(config, mqsarImpl)
}
