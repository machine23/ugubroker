package natsbroker_test

import (
	"context"
	"encoding/json"
	"sync/atomic"
	"testing"
	"time"

	"github.com/machine23/ugubroker/natsbroker"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/require"
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

func TestSubscribe(t *testing.T) {
	ctx := context.Background()

	c, err := startContainer(ctx)
	require.NoError(t, err)
	defer stopContainer(ctx, c)

	// t.Log(c.URI)
	t.Log(c.ConnectionStr)

	nc, err := nats.Connect(c.ConnectionStr)
	require.NoError(t, err)
	defer nc.Close()

	js, err := jetstream.New(nc)
	require.NoError(t, err)

	_, err = js.CreateOrUpdateStream(context.TODO(), jetstream.StreamConfig{
		Name:        "teststream",
		Description: "test",
		Subjects:    []string{"test.>"},
	})
	require.NoError(t, err)

	broker, err := natsbroker.NewSubService(natsbroker.SubServiceConfig{
		ConnectionStr:  c.ConnectionStr,
		ClientName:     "test",
		ConsumerName:   "testconsumer",
		StreamName:     "teststream",
		NumWorkers:     3,
		FilterSubjects: []string{"test.added", "test.updated"},
	})
	require.NoError(t, err)
	defer broker.Close()

	addedCount := atomic.Int32{}
	updatedCount := atomic.Int32{}

	err = broker.Subscribe("test.added", func(ctx context.Context, raw []byte) error {
		// t.Log(string(raw))
		// time.Sleep(100 * time.Millisecond)
		addedCount.Add(1)
		msg := map[string]interface{}{}
		err := json.Unmarshal(raw, &msg)
		require.NoError(t, err)

		msgType, ok := msg["type"].(string)
		require.True(t, ok)
		require.Equal(t, "added", msgType)

		return nil
	})
	require.NoError(t, err)
	err = broker.Subscribe("test.updated", func(ctx context.Context, raw []byte) error {
		// time.Sleep(100 * time.Millisecond)
		updatedCount.Add(1)
		msg := map[string]interface{}{}
		err := json.Unmarshal(raw, &msg)
		require.NoError(t, err)

		msgType, ok := msg["type"].(string)
		require.True(t, ok)
		require.Equal(t, "updated", msgType)
		return nil
	})
	require.NoError(t, err)

	_, err = js.Publish(context.TODO(), "test.added", []byte(`{"id":"1","username":"user1","type":"added"}`))
	require.NoError(t, err)
	_, err = js.Publish(context.TODO(), "test.updated", []byte(`{"id":"2","username":"user1","type":"updated"}`))
	require.NoError(t, err)
	_, err = js.Publish(context.TODO(), "test.added", []byte(`{"id":"3","username":"user1","type":"added"}`))
	require.NoError(t, err)
	_, err = js.Publish(context.TODO(), "test.updated", []byte(`{"id":"4","username":"user1","type":"updated"}`))
	require.NoError(t, err)
	_, err = js.Publish(context.TODO(), "test.added", []byte(`{"id":"5","username":"user1","type":"added"}`))
	require.NoError(t, err)
	_, err = js.Publish(context.TODO(), "test.added", []byte(`{"id":"6","username":"user1","type":"added"}`))
	require.NoError(t, err)
	_, err = js.Publish(context.TODO(), "test.updated", []byte(`{"id":"7","username":"user1","type":"updated"}`))
	require.NoError(t, err)
	_, err = js.Publish(context.TODO(), "test.added", []byte(`{"id":"8","username":"user1","type":"added"}`))
	require.NoError(t, err)
	_, err = js.Publish(context.TODO(), "test.updated", []byte(`{"id":"9","username":"user1","type":"updated"}`))
	require.NoError(t, err)
	_, err = js.Publish(context.TODO(), "test.added", []byte(`{"id":"10","username":"user1","type":"added"}`))
	require.NoError(t, err)
	_, err = js.Publish(context.TODO(), "test.added", []byte(`{"id":"11","username":"user1","type":"added"}`))
	require.NoError(t, err)
	_, err = js.Publish(context.TODO(), "test.updated", []byte(`{"id":"12","username":"user1","type":"updated"}`))
	require.NoError(t, err)
	_, err = js.Publish(context.TODO(), "test.added", []byte(`{"id":"13","username":"user1","type":"added"}`))
	require.NoError(t, err)
	_, err = js.Publish(context.TODO(), "test.updated", []byte(`{"id":"14","username":"user1","type":"updated"}`))
	require.NoError(t, err)
	_, err = js.Publish(context.TODO(), "test.added", []byte(`{"id":"15","username":"user1","type":"added"}`))
	require.NoError(t, err)
	_, err = js.Publish(context.TODO(), "test.added", []byte(`{"id":"16","username":"user1","type":"added"}`))
	require.NoError(t, err)
	_, err = js.Publish(context.TODO(), "test.updated", []byte(`{"id":"17","username":"user1","type":"updated"}`))
	require.NoError(t, err)
	_, err = js.Publish(context.TODO(), "test.added", []byte(`{"id":"18","username":"user1","type":"added"}`))
	require.NoError(t, err)
	_, err = js.Publish(context.TODO(), "test.updated", []byte(`{"id":"19","username":"user1","type":"updated"}`))
	require.NoError(t, err)
	_, err = js.Publish(context.TODO(), "test.some", []byte(`{"id":"20","username":"user1","type":"some"}`))
	require.NoError(t, err)

	time.Sleep(1 * time.Second)

	require.Equal(t, int32(11), addedCount.Load())
	require.Equal(t, int32(8), updatedCount.Load())
	// broker.Close()
}
