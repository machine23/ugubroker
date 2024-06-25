package ugubroker

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

var ErrNoHandlerFound = errors.New("no handler found for topic")

type Message struct {
	Topic string
	Data  []byte
}

type MessageHandler interface {
	ServeMessage(context.Context, Message) error
}

type MessageHandlerFunc func(context.Context, Message) error

func (f MessageHandlerFunc) ServeMessage(ctx context.Context, m Message) error {
	return f(ctx, m)
}

type MessageMux struct {
	handlers map[string]MessageHandler
	mu       sync.RWMutex
}

func NewMessageMux() *MessageMux {
	return &MessageMux{
		handlers: make(map[string]MessageHandler),
	}
}

func (mux *MessageMux) Subscribe(topic string, handler MessageHandler) {
	mux.mu.Lock()
	defer mux.mu.Unlock()

	mux.handlers[topic] = handler
}

func (mux *MessageMux) SubscribeFunc(topic string, handlerFunc func(context.Context, Message) error) {
	mux.Subscribe(topic, MessageHandlerFunc(handlerFunc))
}

func (mux *MessageMux) Handler(topic string) MessageHandler {
	if topic == "" {
		return nil
	}

	mux.mu.RLock()
	defer mux.mu.RUnlock()

	return mux.handlers[topic]
}

func (mux *MessageMux) ServeMessage(ctx context.Context, m Message) error {
	h := mux.Handler(m.Topic)
	if h == nil {
		return fmt.Errorf("%w: %s", ErrNoHandlerFound, m.Topic)
	}

	return h.ServeMessage(ctx, m)
}
