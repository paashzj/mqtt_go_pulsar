package main

import "fmt"

type ExampleMqsarImpl struct {
}

func (e ExampleMqsarImpl) MqttAuth(username, password, clientId string) (bool, error) {
	return true, nil
}

func (e ExampleMqsarImpl) MqttProduceTopic(username, clientId, topic string) (string, error) {
	return fmt.Sprintf("persistent://public/default/%s", topic), nil
}

func (e ExampleMqsarImpl) MqttConsumeTopic(username, clientId, topic string) (string, error) {
	return fmt.Sprintf("persistent://public/default/%s", topic), nil
}
