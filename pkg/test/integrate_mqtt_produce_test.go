package test

import (
	"context"
	"github.com/apache/pulsar-client-go/pulsar"
	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

// TestMqttProducer steps:
// - create pulsar
// - create mqsar
// - create pulsar consumer
// - create mqtt producer
// - mqtt produce message
// - pulsar consumer check message
func TestMqttProducer(t *testing.T) {
	setupPulsar()
	port := setupMqsar()
	pulsarClient, err := pulsar.NewClient(pulsar.ClientOptions{
		URL: "pulsar://localhost:6650",
	})
	if err != nil {
		panic(err)
	}
	mqttTopic := "mqtt-topic"
	pulsarTopic := mqttProduceTopic(mqttTopic)
	ops := mqtt.NewClientOptions().SetUsername("username").SetClientID("foo").AddBroker(MqttConnAddr(port))
	mqttCli := mqtt.NewClient(ops)
	token := mqttCli.Connect()
	token.Wait()
	consumer, err := pulsarClient.Subscribe(pulsar.ConsumerOptions{
		Topic:            pulsarTopic,
		SubscriptionName: "mqtt-produce-test",
	})
	if err != nil {
		panic(err)
	}
	token = mqttCli.Publish(mqttTopic, 0, true, "mqtt-msg")
	token.Wait()
	ctx, cancel := context.WithTimeout(context.TODO(), 5*time.Second)
	defer cancel()
	message, err := consumer.Receive(ctx)
	if err != nil {
		t.Error(err)
	}
	assert.Equal(t, "mqtt-msg", string(message.Payload()))
}
