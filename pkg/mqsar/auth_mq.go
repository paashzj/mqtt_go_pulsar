package mqsar

import (
	"github.com/fhmq/hmq/broker"
	"github.com/fhmq/hmq/plugins/auth"
	"github.com/sirupsen/logrus"
)

type pulsarAuthMq struct {
	server Server
}

func newPulsarAuthMq(server Server) auth.Auth {
	return &pulsarAuthMq{
		server: server,
	}
}

func (auth *pulsarAuthMq) CheckACL(action, clientID, username, ip, topic string) bool {
	switch action {
	case broker.PUB:
		_, err := auth.server.MqttProduceTopic(username, clientID, topic)
		if err != nil {
			return false
		}
	case broker.SUB:
		_, err := auth.server.MqttConsumeTopic(username, clientID, topic)
		if err != nil {
			return false
		}
	}
	return true
}

func (auth *pulsarAuthMq) CheckConnect(clientID, username, password string) bool {
	mqttAuth, err := auth.server.MqttAuth(username, password, clientID)
	if err != nil {
		logrus.Error("check mqtt authentication failed ", err)
		return false
	}
	return mqttAuth
}
