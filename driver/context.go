package driver

import (
	"context"

	"github.com/dolthub/go-mysql-server/sql"
)

type ContextBuilder interface {
	NewContext(context.Context, *Conn, ...sql.ContextOption) (*sql.Context, error)
}

type DefaultContextBuilder struct{}

func (DefaultContextBuilder) NewContext(ctx context.Context, conn *Conn, opts ...sql.ContextOption) (*sql.Context, error) {
	return sql.NewContext(ctx, opts...), nil
}
