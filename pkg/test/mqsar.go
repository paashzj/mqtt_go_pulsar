package test

import "github.com/paashzj/mqtt_go_pulsar/pkg/mqsar"

func setupMqsar() {
	err := setupMqsarInternal()
	if err != nil {
		panic(err)
	}
}

func setupMqsarInternal() error {
	config := &mqsar.Config{}
	config.PulsarConfig = mqsar.PulsarConfig{}
	config.PulsarConfig.Host = "localhost"
	config.PulsarConfig.HttpPort = 8080
	config.PulsarConfig.TcpPort = 6650
	mqsarImpl := &MqsarImpl{}
	return mqsar.Run(config, mqsarImpl)
}
