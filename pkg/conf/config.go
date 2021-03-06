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

package conf

import "time"

type Config struct {
	MqttConfig   MqttConfig
	HttpConfig   HttpConfig
	PulsarConfig PulsarConfig
	TraceConfig  TraceConfig
}

type MqttConfig struct {
	Host            string
	Port            int
	Qos1NoWaitReply bool
}

type HttpConfig struct {
	Disable      bool
	Host         string
	Port         int
	DisablePprof bool
}

type PulsarConfig struct {
	Host          string
	HttpPort      int
	TcpPort       int
	ProduceConfig PulsarProduceConfig
	ConsumeConfig PulsarConsumeConfig
}

type PulsarProduceConfig struct {
	DisableRoutinePool      bool
	RoutinePoolSize         int
	DisableBatching         bool
	BatchingMaxPublishDelay time.Duration
	SendTimeout             time.Duration
	MaxPendingMessages      int
}

type PulsarConsumeConfig struct {
}

type TraceConfig struct {
	DisableTracing bool
	SkywalkingHost string
	SkywalkingPort int
	SampleRate     float64
}
