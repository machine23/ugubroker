package natsbroker

import (
	"fmt"

	"github.com/nats-io/nats.go"
)

type ConnectionConfig struct {
	ConnectionStr string `env:"NATS_CONNECTION_STR"`
	ClientName    string `env:"NATS_CLIENT_NAME"`
}

func NewConnection(conf ConnectionConfig) (*nats.Conn, error) {
	nc, err := nats.Connect(conf.ConnectionStr, nats.Name(conf.ClientName))
	if err != nil {
		return nil, fmt.Errorf("failed to connect to NATS: %w", err)
	}

	return nc, nil
}
