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

package sky

import (
	"context"
	"errors"
	"fmt"
	"github.com/SkyAPM/go2sky"
	"github.com/SkyAPM/go2sky/propagation"
	"github.com/SkyAPM/go2sky/reporter"
	"github.com/hashicorp/go-uuid"
	"github.com/paashzj/mqtt_go_pulsar/pkg/conf"
	"github.com/sirupsen/logrus"
	"os"
)

type NoErrorTracer struct {
	enableTrace bool
	tracer      *go2sky.Tracer
}

func NewTracer(config conf.TraceConfig) *NoErrorTracer {
	n := &NoErrorTracer{}
	n.enableTrace = false
	if !config.DisableTracing {
		grpcReporter, err := reporter.NewGRPCReporter(fmt.Sprintf("%s:%d", config.SkywalkingHost, config.SkywalkingPort))
		if err != nil {
			logrus.Error("skywalking init error, fall back to no trace")
		} else {
			name := instanceName()
			logrus.Info("start skywalking with instance name ", name)
			t, err := go2sky.NewTracer("mqtt_go_pulsar", go2sky.WithReporter(grpcReporter), go2sky.WithSampler(config.SampleRate), go2sky.WithInstance(name))
			if err != nil {
				logrus.Error("skywalking init error, fall back to no trace")
				n.enableTrace = false
			} else {
				n.tracer = t
				n.enableTrace = true
			}
		}
	}
	return n
}

func (n *NoErrorTracer) CreateEntrySpan(ctx context.Context, operationName string, extractor propagation.Extractor) (go2sky.Span, context.Context, error) {
	if !n.enableTrace {
		return nil, nil, errors.New("disable tracing now")
	}
	return n.tracer.CreateEntrySpan(ctx, operationName, extractor)
}

func (n *NoErrorTracer) CreateLocalSpan(ctx context.Context) (go2sky.Span, context.Context, error) {
	if !n.enableTrace {
		return nil, nil, errors.New("disable tracing now")
	}
	return n.tracer.CreateLocalSpan(ctx)
}

func instanceName() string {
	hostname, err := os.Hostname()
	if err == nil {
		return hostname
	}
	generateUUID, err := uuid.GenerateUUID()
	if err == nil {
		return generateUUID
	}
	return "mqtt-go-pulsar"
}
