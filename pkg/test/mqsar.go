package test

import "github.com/paashzj/mqtt_go_pulsar/pkg/mqsar"

func setupMqsar() (*mqsar.Broker, int) {
	port, err := AcquireUnusedPort()
	if err != nil {
		panic(err)
	}
	broker, err := setupMqsarInternal(port)
	if err != nil {
		panic(err)
	}
	return broker, port
}

func setupMqsarInternal(port int) (*mqsar.Broker, error) {
	config := &mqsar.Config{}
	config.MqttConfig = mqsar.MqttConfig{}
	config.MqttConfig.Port = port
	config.PulsarConfig = mqsar.PulsarConfig{}
	config.PulsarConfig.Host = "localhost"
	config.PulsarConfig.HttpPort = 8080
	config.PulsarConfig.TcpPort = 6650
	mqsarImpl := &MqsarImpl{}
	return mqsar.Run(config, mqsarImpl)
}
