package natsbroker_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/machine23/ugubroker/v2/natsbroker"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/require"
)

func TestPublish(t *testing.T) {
	ctx := context.Background()

	c, err := startContainer(ctx)
	require.NoError(t, err)
	defer stopContainer(ctx, c)

	pub, err := natsbroker.NewPubService(natsbroker.PubServiceConfig{
		ConnectionStr: c.ConnectionStr,
		ClientName:    "test-pub",
		StreamName:    "testpub",
		StreamSubject: "testpub.>",
	})
	require.NoError(t, err)

	nc, _ := nats.Connect(c.ConnectionStr)
	defer nc.Close()

	js, _ := jetstream.New(nc)

	stream, _ := js.Stream(context.Background(), "testpub")
	cons, _ := stream.CreateOrUpdateConsumer(context.Background(), jetstream.ConsumerConfig{})

	wg := sync.WaitGroup{}
	wg.Add(11)

	receivedCount := atomic.Int32{}
	cc, _ := cons.Consume(func(msg jetstream.Msg) {
		msg.Ack()
		receivedCount.Add(1)
		require.Equal(t, []byte("test message 1"), msg.Data())
		wg.Done()
	})

	require.NoError(t, pub.Publish(ctx, "testpub.1", []byte("test message 1")))

	time.Sleep(100 * time.Millisecond)

	for range 10 {
		require.NoError(t, pub.Publish(ctx, "testpub.aaa", []byte("test message 1")))
	}

	wg.Wait()
	cc.Stop()

	require.Equal(t, int32(11), receivedCount.Load())
}
