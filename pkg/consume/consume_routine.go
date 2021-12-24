package consume

import (
	"context"
	"github.com/apache/pulsar-client-go/pulsar"
	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/paashzj/mqtt_go_pulsar/pkg/module"
	"github.com/sirupsen/logrus"
)

type RoutineContext struct {
	quit chan bool
}

func StartConsumeRoutine(topicKey module.MqttTopicKey, consumer pulsar.Consumer) *RoutineContext {
	quit := make(chan bool)
	go func() {
		for {
			select {
			case <-quit:
				return
			default:
				receiveMsg, err := consumer.Receive(context.TODO())
				if err != nil {
					logrus.Error("receive error is ", err)
				}
				ops := mqtt.NewClientOptions().SetUsername("broker").SetClientID("broker").AddBroker("tcp://localhost:1883")
				mqttCli := mqtt.NewClient(ops)
				token := mqttCli.Connect()
				token.Wait()
				token = mqttCli.Publish(topicKey.Topic, 0, true, receiveMsg.Payload())
				token.Wait()
				mqttCli.Disconnect(0)
				if err != nil {
					logrus.Error("receive error is ", err)
				} else {
					consumer.Ack(receiveMsg)
				}
			}
		}
	}()
	return &RoutineContext{quit: quit}
}

func StopConsumeRoutine(context *RoutineContext) {
	context.quit <- true
}
