package test

import (
	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/sirupsen/logrus"
	"testing"
)

func TestSetupPulsar(t *testing.T) {
	setupPulsar()
	// start pulsar client
	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL: "pulsar://localhost:6650",
	})
	if err != nil {
		logrus.Error("pulsar client create error")
		t.Error(err)
	}
	client.Close()
	stopPulsar()
}
