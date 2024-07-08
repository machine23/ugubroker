package middleware

import (
	"context"
	"log/slog"
	"time"

	"github.com/machine23/ugubroker/v2"
)

func WithSLog(next ugubroker.MessageHandler) ugubroker.MessageHandler {
	mf := func(ctx context.Context, topic string, data []byte) error {
		start := time.Now()
		err := next.ServeMessage(ctx, topic, data)
		if err != nil {
			slog.Error("failed to process message",
				slog.String("topic", topic),
				slog.String("error", err.Error()),
				slog.Duration("duration", time.Since(start)),
			)
			return err
		}
		slog.Info("message processed",
			slog.String("topic", topic),
			slog.Duration("duration", time.Since(start)),
		)
		return nil
	}
	return ugubroker.MessageHandlerFunc(mf)
}
