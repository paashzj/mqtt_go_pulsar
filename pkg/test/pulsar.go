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
		resp, err := http.Get("http://localhost:8080")
		if err != nil {
			once.Do(startPulsar)
			break
		}
		if resp.StatusCode == 200 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func startPulsar() {
	logrus.Info("start pulsar container")
	err := startPulsarInternal()
	if err != nil {
		panic(err)
	}
	time.Sleep(30 * time.Second)
}

func startPulsarInternal() error {
	cli, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	if err != nil {
		return err
	}
	portSpecs, bindings, err := nat.ParsePortSpecs([]string{"6650", "8080"})
	logrus.Info("port binding is ", bindings)
	resp, err := cli.ContainerCreate(context.TODO(), &container.Config{
		Image:        "ttbb/pulsar:mate",
		Env:          []string{"REMOTE_MODE=false"},
		ExposedPorts: portSpecs,
		Tty:          false,
	}, nil, nil, nil, "mqtt-test-pulsar")
	if err != nil {
		panic(err)
	}

	return cli.ContainerStart(context.TODO(), resp.ID, types.ContainerStartOptions{})
}

func stopPulsar() {
	logrus.Info("stop pulsar container")
	err := stopPulsarInternal()
	if err != nil {
		panic(err)
	}
}

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
