package ugubroker

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

var ErrNoHandlerFound = errors.New("no handler found for topic")

// type Message struct {
// 	Topic string
// 	Data  []byte
// }

type MessageHandler interface {
	ServeMessage(context.Context, string, []byte) error
}

type MessageHandlerFunc func(context.Context, string, []byte) error

func (f MessageHandlerFunc) ServeMessage(ctx context.Context, topic string, m []byte) error {
	return f(ctx, topic, m)
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

func (mux *MessageMux) SubscribeFunc(topic string, handlerFunc func(context.Context, string, []byte) error) {
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

func (mux *MessageMux) ServeMessage(ctx context.Context, topic string, m []byte) error {
	h := mux.Handler(topic)
	if h == nil {
		return fmt.Errorf("%w: %s", ErrNoHandlerFound, topic)
	}

	return h.ServeMessage(ctx, topic, m)
}
