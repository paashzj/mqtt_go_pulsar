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

package main

import (
	"flag"
	"github.com/paashzj/mqtt_go_pulsar/pkg/mqsar"
)

var (
	mqttPort = flag.Int("mqtt_port", 1883, "mqtt listen port")
)

var (
	httpHost = flag.String("http_host", "localhost", "http listen host")
	httpPort = flag.Int("http_port", 21001, "http listen port")
)

var (
	pulsarHost     = flag.String("pulsar_host", "localhost", "pulsar host")
	pulsarHttpPort = flag.Int("pulsar_http_port", 8080, "pulsar http port")
	pulsarTcpPort  = flag.Int("pulsar_tcp_port", 6650, "pulsar tcp port")
)

func main() {
	flag.Parse()
	config := &mqsar.Config{}
	config.MqttConfig = mqsar.MqttConfig{}
	config.MqttConfig.Port = *mqttPort
	config.MqttConfig.Qos1NoWaitReply = true
	config.HttpConfig = mqsar.HttpConfig{}
	config.HttpConfig.Host = *httpHost
	config.HttpConfig.Port = *httpPort
	config.PulsarConfig = mqsar.PulsarConfig{}
	config.PulsarConfig.Host = *pulsarHost
	config.PulsarConfig.HttpPort = *pulsarHttpPort
	config.PulsarConfig.TcpPort = *pulsarTcpPort
	e := &ExampleMqsarImpl{}
	err := mqsar.RunFront(config, e)
	if err != nil {
		panic(err)
	}
}
