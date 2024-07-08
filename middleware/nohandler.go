package middleware

import (
	"context"
	"errors"

	"github.com/machine23/ugubroker/v2"
)

func SkipNoHandlerError(next ugubroker.MessageHandler) ugubroker.MessageHandler {
	wh := func(ctx context.Context, topic string, data []byte) error {
		err := next.ServeMessage(ctx, topic, data)
		if errors.Is(err, ugubroker.ErrNoHandlerFound) {
			return nil
		}
		return err
	}

	return ugubroker.MessageHandlerFunc(wh)
}
