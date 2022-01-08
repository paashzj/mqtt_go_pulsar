package test

import (
	"context"
	"github.com/apache/pulsar-client-go/pulsar"
	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

// TestMqttConsumer steps:
// - create pulsar
// - create mqsar
// - create pulsar producer
// - create mqtt consumer
// - pulsar produce message
// - mqtt consumer check message
func TestMqttConsumer(t *testing.T) {
	setupPulsar()
	_, port := setupMqsar()
	pulsarClient, err := pulsar.NewClient(pulsar.ClientOptions{
		URL: "pulsar://localhost:6650",
	})
	if err != nil {
		panic(err)
	}
	mqttTopic := "mqtt-topic"
	pulsarTopic := mqttConsumeTopic(mqttTopic)
	ops := mqtt.NewClientOptions().SetUsername("username").SetClientID("foo").AddBroker(MqttConnAddr(port))
	mqttCli := mqtt.NewClient(ops)
	token := mqttCli.Connect()
	token.Wait()
	producer, err := pulsarClient.CreateProducer(pulsar.ProducerOptions{
		Topic: pulsarTopic,
	})
	if err != nil {
		t.Error(err)
		return
	}
	channel := make(chan string)
	token = mqttCli.Subscribe(mqttTopic, 0, func(client mqtt.Client, message mqtt.Message) {
		logrus.Info("mqtt receive message ", message.MessageID())
		channel <- string(message.Payload())
	})
	token.Wait()
	err = token.Error()
	if err != nil {
		t.Error(err)
		return
	}
	logrus.Info("mqtt subscribe topic success ", mqttTopic)
	ctx, cancel := context.WithTimeout(context.TODO(), 3*time.Second)
	defer cancel()
	messageID, err := producer.Send(ctx, &pulsar.ProducerMessage{
		Payload: []byte("test-msg"),
	})
	logrus.Info("message id is ", messageID)
	if err != nil {
		t.Error(err)
	}
	select {
	case msg := <-channel:
		assert.Equal(t, "test-msg", msg)
		return
	case <-time.After(5 * time.Second):
		t.Error("test timed out")
	}
}
