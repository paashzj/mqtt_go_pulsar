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
	"fmt"
	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/fhmq/hmq/broker"
	"github.com/gin-contrib/pprof"
	"github.com/gin-gonic/gin"
	"github.com/paashzj/mqtt_go_pulsar/pkg/conf"
	"github.com/paashzj/mqtt_go_pulsar/pkg/metrics"
	"github.com/paashzj/mqtt_go_pulsar/pkg/service"
	"github.com/paashzj/mqtt_go_pulsar/pkg/sky"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/sirupsen/logrus"
	"os"
	"os/signal"
	"strconv"
)

type Broker struct {
	mqttBroker *broker.Broker
}

func (b *Broker) DisConnClientByClientId(clientId string) {
	b.mqttBroker.DisConnClientByClientId(clientId)
}

func RunFront(config *conf.Config, impl Server) (err error) {
	_, err = Run(config, impl)
	if err != nil {
		return
	}
	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt)
	for {
		<-interrupt
		return nil
	}
}

func Run(config *conf.Config, impl Server) (b *Broker, err error) {
	// gin http server
	if !config.HttpConfig.Disable {
		metrics.Init()
		router := gin.Default()
		if !config.HttpConfig.DisablePprof {
			pprof.Register(router)
		}
		router.GET("/metrics", prometheusHandler())
		go func() {
			err := router.Run(fmt.Sprintf("%s:%d", config.HttpConfig.Host, config.HttpConfig.Port))
			if err != nil {
				logrus.Error("run http server error ", err)
			}
		}()
	}
	// tracer
	tracer := sky.NewTracer(config.TraceConfig)
	// mqtt broker
	mqttConfig := &broker.Config{}
	mqttConfig.Host = config.MqttConfig.Host
	mqttConfig.Port = strconv.Itoa(config.MqttConfig.Port)
	clientOptions := pulsar.ClientOptions{}
	clientOptions.URL = fmt.Sprintf("pulsar://%s:%d", config.PulsarConfig.Host, config.PulsarConfig.TcpPort)
	mqttConfig.Plugin.Bridge, err = newPulsarBridgeMq(config.MqttConfig, config.PulsarConfig, clientOptions, impl, tracer)
	if err != nil {
		return nil, err
	}
	mqttConfig.Plugin.Auth = newPulsarAuthMq(impl)
	newBroker, err := broker.NewBroker(mqttConfig)
	if err != nil {
		return nil, err
	}
	newBroker.Start()
	service.SetMqttBroker(newBroker)
	return &Broker{mqttBroker: newBroker}, nil
}

func prometheusHandler() gin.HandlerFunc {
	h := promhttp.Handler()
	return func(c *gin.Context) {
		h.ServeHTTP(c.Writer, c.Request)
	}
}
