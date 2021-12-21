package consume

import (
	"context"
	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/fhmq/hmq/plugins/bridge"
	"github.com/paashzj/mqtt_go_pulsar/pkg/module"
	"github.com/sirupsen/logrus"
)

type RoutineContext struct {
	quit chan bool
}

func StartConsumeRoutine(mq bridge.BridgeMQ, sessionKey module.MqttSessionKey, consumer pulsar.Consumer) *RoutineContext {
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
				elements := &bridge.Elements{ClientID: sessionKey.ClientId, Username: sessionKey.Username, Payload: string(receiveMsg.Payload())}
				err = mq.Publish(elements)
				if err != nil {
					logrus.Error("receive error is ", err)
				}
			}
		}
	}()
	return &RoutineContext{quit: quit}
}

func StopConsumeRoutine(context *RoutineContext) {
	context.quit <- true
}
