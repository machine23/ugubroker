package natsbroker_test

import (
	"context"
	"encoding/json"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/machine23/ugubroker/v2"
	"github.com/machine23/ugubroker/v2/middleware"
	"github.com/machine23/ugubroker/v2/natsbroker"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/rand"
)

func TestNATSConsumer(t *testing.T) {
	ctx := context.Background()

	c, nc, js := setupTestEnvironment(t, ctx)
	defer cleanupTestEnvironment(t, ctx, c, nc)

	t.Run("subscribe", func(t *testing.T) {
		// Create a new message mux
		ah := &addedHandler{t: t}
		uh := &updatedHandler{t: t}

		mux := ugubroker.NewMessageMux()
		mux.Subscribe("test.added", ah)
		mux.Subscribe("test.updated", uh)

		// Create a new NATSConsumer
		consumer, err := natsbroker.NewNATSConsumer(natsbroker.NATSConsumerConfig{
			ConnectionStr:  c.ConnectionStr,
			ClientName:     "test",
			ConsumerName:   "testconsumer",
			StreamName:     "teststream",
			NumWorkers:     3,
			FilterSubjects: []string{"test.>"},
		})
		require.NoError(t, err)

		consumer.Consume(middleware.WithSLog(mux))

		expAdded, expUpdated, _ := publishTestMessages(t, 100, js)

		time.Sleep(1 * time.Second)

		consumer.Close()

		require.Equal(t, expAdded, ah.AddedCount.Load())
		require.Equal(t, expUpdated, uh.UpdatedCount.Load())
	})

	t.Run("subscribeFunc", func(t *testing.T) {
		// Create a new message mux
		var added, updated atomic.Int32

		mux := ugubroker.NewMessageMux()
		mux.SubscribeFunc("test.added", func(_ context.Context, topic string, _ []byte) error {
			require.Equal(t, "test.added", topic)
			added.Add(1)
			return nil
		})
		mux.SubscribeFunc("test.updated", func(_ context.Context, topic string, _ []byte) error {
			require.Equal(t, "test.updated", topic)
			updated.Add(1)
			return nil
		})

		// Create a new NATSConsumer
		consumer, err := natsbroker.NewNATSConsumer(natsbroker.NATSConsumerConfig{
			ConnectionStr:  c.ConnectionStr,
			ClientName:     "test2",
			ConsumerName:   "testconsumer2",
			StreamName:     "teststream",
			NumWorkers:     3,
			FilterSubjects: []string{"test.added", "test.updated"},
		})
		require.NoError(t, err)

		consumer.Consume(mux)

		expAdded, expUpdated, _ := publishTestMessages(t, 100, js)
		// expAdded, expUpdated, _ := 3, 3, 3

		time.Sleep(1 * time.Second)

		consumer.Close()

		require.Equal(t, expAdded, added.Load())
		require.Equal(t, expUpdated, updated.Load())

		////////////////////////////////////////////////////////////////////////////////////////////////////////////////
		// Test receiving messages sent while the consumer is closed,
		// but not deleted from the JetStream.
		////////////////////////////////////////////////////////////////////////////////////////////////////////////////

		// publish more messages
		ea2, eu2, _ := publishTestMessages(t, 100, js)
		expAdded += ea2
		expUpdated += eu2

		time.Sleep(1 * time.Second)

		consumer2, err := natsbroker.NewNATSConsumer(natsbroker.NATSConsumerConfig{
			ConnectionStr:  c.ConnectionStr,
			ClientName:     "test2",
			ConsumerName:   "testconsumer2",
			StreamName:     "teststream",
			NumWorkers:     3,
			FilterSubjects: []string{"test.added", "test.updated"},
		})
		require.NoError(t, err)

		consumer2.Consume(mux)

		time.Sleep(1 * time.Second)

		consumer2.Close()

		require.Equal(t, expAdded, added.Load())
		require.Equal(t, expUpdated, updated.Load())
	})
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// helpers
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

func setupTestEnvironment(t *testing.T, ctx context.Context) (*NatsContainer, *nats.Conn, jetstream.JetStream) {
	c, err := startContainer(ctx)
	require.NoError(t, err)

	nc, err := nats.Connect(c.ConnectionStr)
	require.NoError(t, err)

	js, err := jetstream.New(nc)
	require.NoError(t, err)

	_, err = js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:        "teststream",
		Description: "test",
		Subjects:    []string{"test.>"},
	})
	require.NoError(t, err)

	return c, nc, js
}

func cleanupTestEnvironment(t *testing.T, ctx context.Context, c *NatsContainer, nc *nats.Conn) {
	err := stopContainer(ctx, c)
	require.NoError(t, err)
	nc.Close()
}

type addedHandler struct {
	t          *testing.T
	AddedCount atomic.Int32
}

func (h *addedHandler) ServeMessage(_ context.Context, topic string, data []byte) error {
	require.Equal(h.t, "test.added", topic)
	h.AddedCount.Add(1)
	msg := map[string]interface{}{}
	err := json.Unmarshal(data, &msg)
	require.NoError(h.t, err)

	msgType, ok := msg["type"].(string)
	require.True(h.t, ok)
	require.Equal(h.t, "added", msgType)

	return nil
}

type updatedHandler struct {
	t            *testing.T
	UpdatedCount atomic.Int32
}

func (h *updatedHandler) ServeMessage(_ context.Context, topic string, data []byte) error {
	require.Equal(h.t, "test.updated", topic)
	h.UpdatedCount.Add(1)
	msg := map[string]interface{}{}
	err := json.Unmarshal(data, &msg)
	require.NoError(h.t, err)

	msgType, ok := msg["type"].(string)
	require.True(h.t, ok)
	require.Equal(h.t, "updated", msgType)

	return nil
}

func publishTestMessages(t *testing.T, n int, js jetstream.JetStream) (int32, int32, int32) {
	tps := []string{"added", "updated", "some"}
	var added, updated, some int32

	for i := range n {
		tp := tps[rand.Intn(len(tps))]
		topic := "test." + tp
		msg := fmt.Sprintf(`{"id":"%d","username":"user1","type":"%s"}`, i, tp)

		_, err := js.Publish(context.TODO(), topic, []byte(msg))
		require.NoError(t, err)

		switch tp {
		case "added":
			added++
		case "updated":
			updated++
		case "some":
			some++
		}
	}

	return added, updated, some
}
