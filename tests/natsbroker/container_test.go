package natsbroker_test

import (
	"context"

	"github.com/testcontainers/testcontainers-go"
	tcnats "github.com/testcontainers/testcontainers-go/modules/nats"
)

type NatsContainer struct {
	*tcnats.NATSContainer
	// testcontainers.Container
	ConnectionStr string
}

func startContainer(ctx context.Context) (*NatsContainer, error) {
	natsC, err := tcnats.RunContainer(ctx, testcontainers.WithImage("nats:latest"),

		tcnats.WithArgument("jetstream", "-DV"),
		// testcontainers.WithWaitStrategy(
		// 	wait.ForLog("JetStream is ready").WithOccurrence(2).WithStartupTimeout(5*time.Second),
		// ),
	)
	if err != nil {
		return nil, err
	}

	connStr, err := natsC.ConnectionString(ctx)
	if err != nil {
		return nil, err
	}

	return &NatsContainer{
		NATSContainer: natsC,
		ConnectionStr: connStr,
	}, nil
}

func stopContainer(ctx context.Context, c *NatsContainer) error {
	return c.Terminate(ctx)
}
