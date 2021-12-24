package test

import "fmt"

type MqsarImpl struct {
}

func (m MqsarImpl) MqttAuth(username, password, clientId string) (bool, error) {
	return true, nil
}

func (m MqsarImpl) MqttProduceTopic(username, clientId, topic string) (string, error) {
	return mqttProduceTopic(topic), nil
}

func mqttProduceTopic(topic string) string {
	return fmt.Sprintf("persistent://public/default/%s", topic)
}

func (m MqsarImpl) MqttConsumeTopic(username, clientId, topic string) (string, error) {
	return mqttConsumeTopic(topic), nil
}

func mqttConsumeTopic(topic string) string {
	return fmt.Sprintf("persistent://public/default/%s", topic)
}
