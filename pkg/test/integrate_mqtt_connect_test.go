package test

import (
	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/stretchr/testify/assert"
	"testing"
)

// TestMqttConnect steps:
// - create pulsar
// - create mqsar
// - create mqtt client
// - mqtt client connect
func TestMqttConnect(t *testing.T) {
	setupPulsar()
	setupMqsar()
	ops := mqtt.NewClientOptions().SetUsername("username").SetClientID("foo").AddBroker("tcp://localhost:1883")
	mqttCli := mqtt.NewClient(ops)
	token := mqttCli.Connect()
	token.Wait()
	assert.True(t, mqttCli.IsConnected())
}
