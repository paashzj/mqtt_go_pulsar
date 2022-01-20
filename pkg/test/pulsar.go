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
	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/client"
	"github.com/docker/go-connections/nat"
	"github.com/sirupsen/logrus"
	"net/http"
	"strings"
	"sync"
	"time"
)

const (
	pulsarContainerName = "mqtt-test-pulsar"
)

func setupPulsar() {
	var once sync.Once
	now := time.Now()
	for {
		if time.Since(now).Minutes() > 3 {
			panic("pulsar still not started")
		}
		resp, err := http.Get("http://localhost:8080/admin/v2/brokers/health")
		if err != nil {
			logrus.Error("connect pulsar error ")
			once.Do(startPulsar)
			time.Sleep(15 * time.Second)
			continue
		}
		if resp.StatusCode == 200 {
			logrus.Info("health check success")
			break
		}
		logrus.Info("resp code is ", resp.StatusCode)
		time.Sleep(5 * time.Second)
	}
}

func startPulsar() {
	logrus.Info("start pulsar container")
	err := startPulsarInternal()
	if err != nil {
		panic(err)
	}
}

func startPulsarInternal() error {
	cli, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	if err != nil {
		return err
	}
	portSpecs, bindings, err := nat.ParsePortSpecs([]string{"6650", "8080"})
	if err != nil {
		return err
	}
	logrus.Info("port binding is ", bindings)
	resp, err := cli.ContainerCreate(context.TODO(), &container.Config{
		Image:        "ttbb/pulsar:mate",
		Env:          []string{"REMOTE_MODE=false"},
		ExposedPorts: portSpecs,
		Tty:          false,
	}, &container.HostConfig{
		PortBindings: nat.PortMap{
			"6650/tcp": []nat.PortBinding{
				{
					HostIP:   "0.0.0.0",
					HostPort: "6650",
				},
			},
			"8080/tcp": []nat.PortBinding{
				{
					HostIP:   "0.0.0.0",
					HostPort: "8080",
				},
			},
		},
	}, nil, nil, "mqtt-test-pulsar")
	if err != nil {
		panic(err)
	}

	return cli.ContainerStart(context.TODO(), resp.ID, types.ContainerStartOptions{})
}

//nolint
func stopPulsar() {
	logrus.Info("stop pulsar container")
	err := stopPulsarInternal()
	if err != nil {
		panic(err)
	}
}

//nolint
func stopPulsarInternal() error {
	cli, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	if err != nil {
		return err
	}
	containerList, err := cli.ContainerList(context.TODO(), types.ContainerListOptions{})
	if err != nil {
		return err
	}
	for _, c := range containerList {
		names := c.Names
		for _, name := range names {
			if strings.Contains(name, pulsarContainerName) {
				duration := 3 * time.Minute
				err := cli.ContainerStop(context.TODO(), c.ID, &duration)
				if err != nil {
					return err
				}
				_ = cli.ContainerRemove(context.TODO(), c.ID, types.ContainerRemoveOptions{
					Force: true,
				})
			}
		}
	}
	return nil
}
