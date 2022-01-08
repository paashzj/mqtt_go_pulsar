package service

import "github.com/fhmq/hmq/broker"

var mqttBroker *broker.Broker

func SetMqttBroker(broker *broker.Broker) {
	mqttBroker = broker
}

func GetMqttBroker() *broker.Broker {
	return mqttBroker
}
