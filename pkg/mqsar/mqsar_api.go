package mqsar

type Server interface {
	MqttProduceTopic(username string, clientId string) (string, error)
	MqttConsumeTopic(username string, clientId string) (string, error)
}
