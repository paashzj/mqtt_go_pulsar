package mqsar

type Server interface {
	MqttAuth(username, password, clientId string) (bool, error)
	MqttProduceTopic(username, clientId, topic string) (string, error)
	MqttConsumeTopic(username, clientId, topic string) (string, error)
}
