package test

import (
	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestMqttBrokerDisconnect(t *testing.T) {
	setupPulsar()
	broker, port := setupMqsar()
	ops := mqtt.NewClientOptions().SetUsername("username").SetClientID("foo").SetAutoReconnect(false).AddBroker(MqttConnAddr(port))
	mqttCli := mqtt.NewClient(ops)
	token := mqttCli.Connect()
	token.Wait()
	assert.True(t, mqttCli.IsConnected())
	broker.DisConnClientByClientId("foo")
	time.Sleep(3 * time.Second)
	assert.False(t, mqttCli.IsConnected())
}
