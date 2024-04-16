package natsbroker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type SubServiceConfig struct {
	ConnectionStr string `env:"NATS_CONNECTION_STR"`
	ClientName    string `env:"NATS_CLIENT_NAME"`
	StreamName    string `env:"NATS_STREAM_NAME" env-default:"ugu"`
	ConsumerName  string `env:"NATS_CONSUMER_NAME"`
	NumWorkers    int    `env:"NATS_NUM_WORKERS"`
}

type SubService struct {
	log    *slog.Logger
	config SubServiceConfig

	nc              *nats.Conn
	js              jetstream.JetStream
	stream          jetstream.Stream
	consumer        jetstream.Consumer
	subs            []jetstream.ConsumeContext
	filterSubjects  []string
	handlers        map[string]func(context.Context, []byte) error
	reuseConnection bool

	mu        sync.RWMutex
	wg        sync.WaitGroup
	isStopped atomic.Bool
	sem       chan struct{}
}

func NewSubService(conf SubServiceConfig) (*SubService, error) {
	connConfig := ConnectionConfig{
		ConnectionStr: conf.ConnectionStr,
		ClientName:    conf.ClientName,
	}

	nc, err := NewConnection(connConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create connection: %w", err)
	}

	return newSubServiceWithConnection(nc, conf)
}

func NewSubServiceUsingConnection(nc *nats.Conn, conf SubServiceConfig) (*SubService, error) {
	ss, err := newSubServiceWithConnection(nc, conf)
	if err != nil {
		return nil, fmt.Errorf("failed to create service: %w", err)
	}

	ss.reuseConnection = true
	return ss, nil
}

func newSubServiceWithConnection(nc *nats.Conn, conf SubServiceConfig) (*SubService, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	js, err := jetstream.New(nc)
	if err != nil {
		return nil, fmt.Errorf("failed to create JetStream: %w", err)
	}

	if conf.NumWorkers == 0 {
		conf.NumWorkers = 1
	}

	stream, err := js.Stream(ctx, conf.StreamName)
	if err != nil {
		return nil, fmt.Errorf("failed to get stream: %w", err)
	}

	log := slog.Default().With("module", "messaging/natssub")

	log.Info("service started",
		slog.String("clientName", conf.ClientName),
		slog.String("consumerName", conf.ConsumerName))

	return &SubService{
		log:            log,
		config:         conf,
		nc:             nc,
		js:             js,
		stream:         stream,
		filterSubjects: []string{},
		handlers:       make(map[string]func(context.Context, []byte) error),
		sem:            make(chan struct{}, conf.NumWorkers),
	}, nil
}

func (s *SubService) Close() {
	s.isStopped.Store(true)

	s.wg.Wait()

	for _, sub := range s.subs {
		sub.Stop()
	}

	if !s.reuseConnection {
		s.nc.Close()
	}

	s.log.Info("service stopped")
}

func (s *SubService) Subscribe(topic string, handler func(context.Context, []byte) error) error {

	if s.isStopped.Load() {
		return fmt.Errorf("service is stopped")
	}

	if err := s.updateConsumer(topic); err != nil {
		return err
	}

	s.mu.Lock()
	s.handlers[topic] = handler
	s.mu.Unlock()

	if err := s.consumeMessages(); err != nil {
		return fmt.Errorf("failed to consume messages: %w", err)
	}

	s.log.Info("subscribed to topic", slog.String("topic", topic))

	return nil
}

func (s *SubService) updateConsumer(topic string) error {
	if topic == "" {
		return fmt.Errorf("topic cannot be empty")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if s.consumer == nil {
		if err := s.createConsumer(ctx, topic); err != nil {
			return err
		}
	}

	return s.updateConsumerConfig(ctx, topic)
}

func (s *SubService) createConsumer(ctx context.Context, topic string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if topic == "" {
		return fmt.Errorf("topic cannot be empty")
	}

	consumer, err := s.stream.Consumer(ctx, s.config.ConsumerName)
	if err != nil {
		if errors.Is(err, jetstream.ErrConsumerNotFound) {
			consumer, err = s.stream.CreateConsumer(ctx, jetstream.ConsumerConfig{
				Name:           s.config.ConsumerName,
				Durable:        s.config.ConsumerName,
				FilterSubjects: []string{topic},
			})
			if err != nil {
				return fmt.Errorf("failed to create consumer: %w", err)
			}
			s.log.Info("created consumer",
				slog.String("consumerName", s.config.ConsumerName),
				slog.String("streamName", s.config.StreamName),
				slog.Any("filterSubjects", []string{topic}))
		} else {
			return fmt.Errorf("failed to get consumer: %w", err)
		}
	}

	s.consumer = consumer
	return nil
}

func (s *SubService) updateConsumerConfig(ctx context.Context, topic string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	consumerInfo, err := s.consumer.Info(ctx)
	if err != nil {
		return fmt.Errorf("failed to get consumer info: %w", err)
	}

	consumerConf := consumerInfo.Config
	if consumerConf.FilterSubjects == nil {
		consumerConf.FilterSubjects = make([]string, 0)
	}
	if !slices.Contains(consumerConf.FilterSubjects, topic) {
		consumerConf.FilterSubjects = append(consumerConf.FilterSubjects, topic)
		consumer, err := s.stream.UpdateConsumer(ctx, consumerConf)
		if err != nil {
			return fmt.Errorf("failed to update consumer: %w", err)
		}

		s.consumer = consumer

		s.log.Info("consumer updated",
			slog.String("consumerName", s.config.ConsumerName),
			slog.String("streamName", s.config.StreamName),
			slog.Any("filterSubjects", consumerConf.FilterSubjects))
	}

	return nil
}

func (s *SubService) consumeMessages() error {

	cctx, err := s.consumer.Consume(func(msg jetstream.Msg) {
		if s.isStopped.Load() {
			if err := msg.NakWithDelay(randomDuration(100*time.Millisecond, 2*time.Second)); err != nil {
				s.log.Error("failed to NAK message", slog.String("error", err.Error()),
					slog.String("path", "consumeMessages -> s.isStopped.Load()"))
			}
			return
		}
		s.wg.Add(1)
		s.sem <- struct{}{}
		go func() {
			defer func() {
				<-s.sem
				s.wg.Done()
			}()
			start := time.Now()
			s.mu.RLock()
			handler, ok := s.handlers[msg.Subject()]
			if !ok {
				s.log.Error("handler not found", slog.String("topic", msg.Subject()))
				if err := msg.NakWithDelay(randomDuration(100*time.Millisecond, 2*time.Second)); err != nil {
					s.log.Error("failed to NAK message", slog.String("error", err.Error()),
						slog.String("path", "consumeMessages -> handler not found"))
				}
				return
			}
			s.mu.RUnlock()

			if err := handler(context.Background(), msg.Data()); err != nil {
				s.log.Error("failed to handle event",
					slog.String("error", err.Error()),
					slog.String("topic", msg.Subject()),
					slog.String("payload", string(msg.Data())),
					slog.String("duration", time.Since(start).String()),
				)
				if err := msg.NakWithDelay(randomDuration(5*time.Second, 30*time.Second)); err != nil {
					s.log.Error("failed to NAK message", slog.String("error", err.Error()),
						slog.String("path", "consumeMessages -> handler"))
				}
				return
			}
			if err := msg.Ack(); err != nil {
				s.log.Error("failed to ACK message", slog.String("error", err.Error()),
					slog.String("path", "consumeMessages -> handler"))
				return
			}
			s.log.Info("event handled", slog.String("topic", msg.Subject()), slog.String("duration", time.Since(start).String()))
		}()
	})

	if err != nil {
		return fmt.Errorf("failed to consume: %w", err)
	}

	s.mu.Lock()
	s.subs = append(s.subs, cctx)
	s.mu.Unlock()

	return nil
}

func randomDuration(minDur, maxDur time.Duration) time.Duration {
	return minDur + time.Duration(rand.Int63n(int64(maxDur-minDur)))
}
