package middleware

import (
	"context"
	"errors"

	"github.com/machine23/ugubroker/v2"
	"golang.org/x/exp/slog"
)

func SkipNoHandlerError(next ugubroker.MessageHandler) ugubroker.MessageHandler {
	wh := func(ctx context.Context, m ugubroker.Message) error {
		err := next.ServeMessage(ctx, m)
		if errors.Is(err, ugubroker.ErrNoHandlerFound) {
			slog.Warn("skip no handler error",
				slog.String("topic", m.Topic),
			)
			return nil
		}
		return err
	}

	return ugubroker.MessageHandlerFunc(wh)
}
