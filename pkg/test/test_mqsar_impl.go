package test

import (
	"fmt"
)

type MqsarImpl struct {
}

func (m MqsarImpl) MqttAuth(username, password, clientId string) (bool, error) {
	if password == "wrong_password" {
		return false, nil
	}
	return true, nil
}

func (m MqsarImpl) MqttProduceTopic(username, clientId, topic string) (string, error) {
	if username == "username" && topic == "wrong-mqtt-topic" {
		return "", fmt.Errorf("Pub Topics Auth failed ")
	}
	return mqttProduceTopic(topic), nil
}

func mqttProduceTopic(topic string) string {
	return fmt.Sprintf("persistent://public/default/%s", topic)
}

func (m MqsarImpl) MqttConsumeTopic(username, clientId, topic string) (string, error) {
	if username == "username" && topic == "wrong-mqtt-topic" {
		return "", fmt.Errorf("Sub Topics Auth failed ")
	}
	return mqttConsumeTopic(topic), nil
}

func mqttConsumeTopic(topic string) string {
	return fmt.Sprintf("persistent://public/default/%s", topic)
}
