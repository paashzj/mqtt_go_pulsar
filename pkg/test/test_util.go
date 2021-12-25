package test

import (
	"fmt"
	"net"
)

func MqttConnAddr(port int) string {
	return fmt.Sprintf("tcp://localhost:%d", port)
}

func AcquireUnusedPort() (int, error) {
	tcpAddr, err := net.ResolveTCPAddr("tcp", "0.0.0.0:0")
	if err != nil {
		return 0, err
	}
	l, err := net.ListenTCP("tcp", tcpAddr)
	if err != nil {
		return 0, err
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port, nil
}
