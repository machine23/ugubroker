package natsbroker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/machine23/ugubroker/v2"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const (
	DefaultCreationTimeout = time.Duration(10 * time.Second)
	DefaultWorkingTimeout  = time.Duration(10 * time.Second)
)

type NATSConsumerConfig struct {
	ConnectionStr  string
	ClientName     string
	StreamName     string
	ConsumerName   string
	NumWorkers     int
	FilterSubjects []string

	CreationTimeout time.Duration
	WorkingTimeout  time.Duration
}

type NATSConsumer struct {
	timeout  time.Duration
	nc       *nats.Conn
	consumer jetstream.Consumer

	reuseConnection bool
	wg              sync.WaitGroup
	isStopped       atomic.Bool
	sem             chan struct{}
	subs            []jetstream.ConsumeContext
}

func NewNATSConsumer(conf NATSConsumerConfig) (*NATSConsumer, error) {
	connConfig := ConnectionConfig{
		ConnectionStr: conf.ConnectionStr,
		ClientName:    conf.ClientName,
	}

	nc, err := NewConnection(connConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create NATS connection: %w", err)
	}

	return newNATSConsumerWithConnection(nc, conf)
}

func NewNATSConsumerUsingConnection(nc *nats.Conn, conf NATSConsumerConfig) (*NATSConsumer, error) {
	consumer, err := newNATSConsumerWithConnection(nc, conf)
	if err != nil {
		return nil, fmt.Errorf("failed to create NATS consumer using connection: %w", err)
	}

	consumer.reuseConnection = true
	return consumer, nil
}

func newNATSConsumerWithConnection(nc *nats.Conn, conf NATSConsumerConfig) (*NATSConsumer, error) {
	if conf.CreationTimeout <= 0 {
		conf.CreationTimeout = DefaultCreationTimeout
	}

	if conf.WorkingTimeout <= 0 {
		conf.WorkingTimeout = DefaultWorkingTimeout
	}

	if conf.NumWorkers <= 0 {
		conf.NumWorkers = 1
	}

	if conf.ConsumerName == "" {
		return nil, errors.New("consumer name cannot be empty")
	}

	if len(conf.FilterSubjects) == 0 {
		return nil, errors.New("filter subjects cannot be empty")
	}

	ctx, cancel := context.WithTimeout(context.Background(), conf.CreationTimeout)
	defer cancel()

	js, err := jetstream.New(nc)
	if err != nil {
		return nil, fmt.Errorf("failed to create JetStream: %w", err)
	}

	stream, err := js.Stream(ctx, conf.StreamName)
	if err != nil {
		return nil, fmt.Errorf("failed to get stream: %w", err)
	}

	consumer, err := stream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
		Name:           conf.ConsumerName,
		Durable:        conf.ConsumerName,
		FilterSubjects: conf.FilterSubjects,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create or update NATS consumer: %w", err)
	}

	return &NATSConsumer{
		timeout:  conf.WorkingTimeout,
		nc:       nc,
		consumer: consumer,
		sem:      make(chan struct{}, conf.NumWorkers),
	}, nil

}

func (n *NATSConsumer) Close() {
	for _, sub := range n.subs {
		sub.Stop()
	}
	n.isStopped.Store(true)
	n.wg.Wait()
	if !n.reuseConnection {
		n.nc.Close()
	}
}

func (n *NATSConsumer) Consume(handler ugubroker.MessageHandler) error {
	cctx, err := n.consumer.Consume(func(msg jetstream.Msg) {
		if n.isStopped.Load() {
			if err := msg.NakWithDelay(10 * time.Second); err != nil {
				slog.Error("failed to NAK message", slog.String("error", err.Error()),
					slog.String("place", "NATSConsumer.Consume -> s.isStopped"))
			}
			return
		}

		n.wg.Add(1)
		n.sem <- struct{}{}

		go func() {
			defer func() {
				<-n.sem
				n.wg.Done()
			}()

			if err := n.serveMessage(msg, handler); err != nil {

				if err := msg.NakWithDelay(randomDuration(5*time.Second, 30*time.Second)); err != nil {
					slog.Error("failed to NAK message",
						slog.String("error", err.Error()),
						slog.String("place", "NATSConsumer.Consume -> serveMessage"))
				}
				return
			}

			if err := msg.Ack(); err != nil {
				slog.Error("failed to ACK message",
					slog.String("error", err.Error()),
					slog.String("place", "NATSConsumer.Consume -> msg.Ack"),
				)
			}
		}()
	})

	if err != nil {
		return fmt.Errorf("failed to consume: %w", err)
	}

	n.subs = append(n.subs, cctx)
	return nil

}

func (n *NATSConsumer) serveMessage(msg jetstream.Msg, handler ugubroker.MessageHandler) error {
	ctx, cancel := context.WithTimeout(context.Background(), n.timeout)
	defer cancel()

	message := ugubroker.Message{
		Topic: msg.Subject(),
		Data:  msg.Data(),
	}

	return handler.ServeMessage(ctx, message)
}
