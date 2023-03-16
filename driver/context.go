package driver

import (
	"context"

	"github.com/dolthub/go-mysql-server/sql"
)

// A ContextBuilder creates SQL contexts.
type ContextBuilder interface {
	// NewContext constructs a sql.Context with the given conn and options set.
	NewContext(context.Context, *Conn, ...sql.ContextOption) (*sql.Context, error)
}

// DefaultContextBuilder creates basic SQL contexts.
type DefaultContextBuilder struct{}

// NewContext calls sql.NewContext.
func (DefaultContextBuilder) NewContext(ctx context.Context, conn *Conn, opts ...sql.ContextOption) (*sql.Context, error) {
	return sql.NewContext(ctx, opts...), nil
}
