package module

type MqttSessionKey struct {
	Username string
	ClientId string
}

type MqttTopicKey struct {
	MqttSessionKey
	Topic string
}
