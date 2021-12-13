package main

import "fmt"

type ExampleMqsarImpl struct {
}

func (e ExampleMqsarImpl) MqttProduceTopic(username string, clientId string) (string, error) {
	return fmt.Sprintf("persistent://public/default/%s-%s-produce", username, clientId), nil
}

func (e ExampleMqsarImpl) MqttConsumeTopic(username string, clientId string) (string, error) {
	return fmt.Sprintf("persistent://public/default/%s-%s-consume", username, clientId), nil
}
