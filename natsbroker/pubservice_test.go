package natsbroker_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/machine23/ugubroker/natsbroker"
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

	require.NoError(t, pub.Publish(ctx, "testpub.1", []byte("test message 1")))

	time.Sleep(100 * time.Millisecond)

	sub, err := natsbroker.NewSubService(natsbroker.SubServiceConfig{
		ConnectionStr: c.ConnectionStr,
		ClientName:    "test-sub",
		ConsumerName:  "testconsumer",
		StreamName:    "testpub",
		NumWorkers:    3,
	})

	require.NoError(t, err)
	defer sub.Close()

	var count atomic.Int32

	require.NoError(t, sub.Subscribe("testpub.aaa", func(ctx context.Context, raw []byte) error {
		require.Equal(t, "test message 1", string(raw))
		count.Add(1)
		return nil
	}))

	for range 10 {
		require.NoError(t, pub.Publish(ctx, "testpub.aaa", []byte("test message 1")))
	}

	time.Sleep(100 * time.Millisecond)
	require.Equal(t, int32(10), count.Load())
}
