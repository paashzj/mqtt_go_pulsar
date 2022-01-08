package consume

import (
	"context"
	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/eclipse/paho.mqtt.golang/packets"
	"github.com/fhmq/hmq/broker"
	"github.com/paashzj/mqtt_go_pulsar/pkg/module"
	"github.com/paashzj/mqtt_go_pulsar/pkg/service"
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
					return
				}
				mqttBroker := service.GetMqttBroker()
				publishPacket := packets.NewControlPacket(packets.Publish).(*packets.PublishPacket)
				publishPacket.TopicName = topicKey.Topic
				publishPacket.Payload = receiveMsg.Payload()
				publishPacket.Qos = broker.QosAtLeastOnce
				publishPacket.Retain = false
				publishPacket.Dup = false
				mqttBroker.PublishMessage(publishPacket)
				consumer.Ack(receiveMsg)
			}
		}
	}()
	return &RoutineContext{quit: quit}
}

func StopConsumeRoutine(context *RoutineContext) {
	context.quit <- true
}
