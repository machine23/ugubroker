package natsbroker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/machine23/ugubroker/v2"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"golang.org/x/exp/rand"
)

const (
	DefaultCreationTimeout = time.Duration(10 * time.Second)
	DefaultWorkingTimeout  = time.Duration(10 * time.Second)
)

type NATSConsumerConfig struct {
	ConnectionStr   string
	ClientName      string
	StreamName      string
	ConsumerName    string
	FilterSubjects  []string
	NumWorkers      int
	CreationTimeout time.Duration
	WorkingTimeout  time.Duration

	// If set to true, the consumer will receive all messages from the stream.
	// If set to false, the consumer will only receive messages that sent after
	// the consumer is created.
	ReceiveAllMsgs bool
}

type NATSConsumer struct {
	consumer        jetstream.Consumer
	nc              *nats.Conn
	sem             chan struct{}
	subs            []jetstream.ConsumeContext
	wg              sync.WaitGroup
	timeout         time.Duration
	reuseConnection bool
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

	start := time.Now()

	consumer, err := stream.Consumer(ctx, conf.ConsumerName)
	if err != nil {
		if errors.Is(err, jetstream.ErrConsumerNotFound) {
			deliverPolicy := jetstream.DeliverByStartTimePolicy
			if conf.ReceiveAllMsgs {
				deliverPolicy = jetstream.DeliverAllPolicy
			}
			consumer, err = stream.CreateConsumer(ctx, jetstream.ConsumerConfig{
				Name:           conf.ConsumerName,
				Durable:        conf.ConsumerName,
				FilterSubjects: conf.FilterSubjects,
				DeliverPolicy:  deliverPolicy,
				OptStartTime:   &start,
			})
			if err != nil {
				return nil, fmt.Errorf("failed to create NATS consumer: %w", err)
			}
		} else {
			return nil, fmt.Errorf("failed to get NATS consumer: %w", err)
		}
	}

	cnsmrInfo, err := consumer.Info(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get consumer info: %w", err)
	}

	if !isFilterSubjectsEqual(cnsmrInfo.Config.FilterSubjects, conf.FilterSubjects) {
		cnf := cnsmrInfo.Config
		cnf.FilterSubjects = conf.FilterSubjects

		consumer, err = stream.UpdateConsumer(ctx, cnf)
		if err != nil {
			return nil, fmt.Errorf("failed to update NATS consumer: %w", err)
		}
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
	n.wg.Wait()
	if !n.reuseConnection {
		n.nc.Close()
	}
}

func (n *NATSConsumer) Consume(handler ugubroker.MessageHandler) error {
	cctx, err := n.consumer.Consume(func(msg jetstream.Msg) {
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

func randomDuration(minDuration, maxDuration time.Duration) time.Duration {
	return minDuration + time.Duration(rand.Int63n(int64(maxDuration-minDuration)))
}

func isFilterSubjectsEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}

	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}

	return true
}
