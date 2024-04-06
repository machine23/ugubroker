package natsbroker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type PubServiceConfig struct {
	ConnectionStr string `env:"NATS_CONNECTION_STR"`
	ClientName    string `env:"NATS_CLIENT_NAME"`
	StreamName    string `env:"NATS_STREAM_NAME" env-default:"ugu"`
	StreamSubject string `env:"NATS_STREAM_SUBJECT"`
}

type PubService struct {
	log    *slog.Logger
	config PubServiceConfig

	nc     *nats.Conn
	js     jetstream.JetStream
	stream jetstream.Stream

	reuseConnection bool
}

func NewPubService(conf PubServiceConfig) (*PubService, error) {
	connConfig := ConnectionConfig{
		ConnectionStr: conf.ConnectionStr,
		ClientName:    conf.ClientName,
	}

	nc, err := NewConnection(connConfig)
	if err != nil {
		return nil, err
	}

	return newPubServiceWithConnection(nc, conf)
}

func NewPubServiceUsingConnection(nc *nats.Conn, conf PubServiceConfig) (*PubService, error) {
	ps, err := newPubServiceWithConnection(nc, conf)
	if err != nil {
		return nil, err
	}

	ps.reuseConnection = true

	return ps, nil
}

func newPubServiceWithConnection(nc *nats.Conn, conf PubServiceConfig) (*PubService, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	js, err := jetstream.New(nc)
	if err != nil {
		return nil, err
	}

	stream, err := createOrUpdateStream(ctx, js, conf)
	if err != nil {
		return nil, fmt.Errorf("failed to create or update stream: %w", err)
	}

	return &PubService{
		log:    slog.Default().With(slog.String("module", "pubservice")),
		config: conf,
		nc:     nc,
		js:     js,
		stream: stream,
	}, nil
}

func createOrUpdateStream(ctx context.Context, js jetstream.JetStream, conf PubServiceConfig) (jetstream.Stream, error) {
	stream, err := js.Stream(ctx, conf.StreamName)
	if err != nil {
		if errors.Is(err, jetstream.ErrStreamNotFound) {
			stream, err = js.CreateStream(ctx, jetstream.StreamConfig{
				Name:     conf.StreamName,
				Subjects: []string{conf.StreamSubject},
			})
			if err != nil {
				if errors.Is(err, jetstream.ErrStreamNameAlreadyInUse) {
					stream, err = js.Stream(ctx, conf.StreamName)
					if err != nil {
						return nil, fmt.Errorf("failed to get stream: %w", err)
					}
				} else {
					return nil, fmt.Errorf("failed to create stream: %w", err)
				}
			}
		} else {
			return nil, fmt.Errorf("failed to get stream: %w", err)
		}
	}

	info, err := stream.Info(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get stream info: %w", err)
	}

	streamConfig := info.Config
	if !slices.Contains(streamConfig.Subjects, conf.StreamSubject) {
		streamConfig.Subjects = append(streamConfig.Subjects, conf.StreamSubject)

		stream, err = js.UpdateStream(ctx, streamConfig)
		if err != nil {
			return nil, fmt.Errorf("failed to update stream: %w", err)
		}
	}

	return stream, err
}

func (s *PubService) Publish(ctx context.Context, subject string, data []byte) error {
	_, err := s.js.Publish(ctx, subject, data)
	if err != nil {
		return fmt.Errorf("failed to publish message: %w", err)
	}

	return nil
}

func (s *PubService) Close() {
	if s.reuseConnection {
		return
	}

	s.nc.Close()
}
