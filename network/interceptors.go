package network

import (
	"connectrpc.com/connect"
	"context"
	"log/slog"
	"net/http"
	"strings"
	"time"
)

func LoggingInterceptor(logger *slog.Logger) connect.UnaryInterceptorFunc {

	return func(next connect.UnaryFunc) connect.UnaryFunc {

		return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {

			start := time.Now()
			logger.Debug("Incoming request",
				slog.String("method", req.Spec().Procedure),
				slog.String("peer", req.Peer().Addr),
			)

			resp, err := next(ctx, req)
			d := time.Since(start)

			if err != nil {
				logger.Error("Error processing request",
					slog.String("method", req.Spec().Procedure),
					slog.String("peer", req.Peer().Addr),
					slog.String("error", err.Error()),
					slog.Duration("duration", d))
				return resp, err
			}

			logger.Debug("Completed request",
				slog.String("method", req.Spec().Procedure),
				slog.String("peer", req.Peer().Addr),
				slog.Duration("duration", d),
			)
			return resp, nil
		}

	}
}

// extractMetadata extracts headers to slog.Group for structured logging
func extractMetadata(md http.Header) []any {
	fields := make([]any, 0, len(md)*2)
	for key, value := range md {
		fields = append(fields, slog.String(key, strings.Join(value, "|")))
	}
	return fields
}
