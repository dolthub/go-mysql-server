package driver

import (
	"context"

	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/sql"
)

// A SessionBuilder creates SQL sessions.
type SessionBuilder interface {
	NewSession(ctx context.Context, id uint32, conn *Connector) (sql.Session, error)
}

// DefaultSessionBuilder creates basic SQL sessions.
type DefaultSessionBuilder struct {
	provider *memory.DbProvider
}

func NewDefaultSessionBuilder() *DefaultSessionBuilder {
	return &DefaultSessionBuilder{
		provider: memory.NewDBProvider(),
	}
}

// NewSession calls sql.NewBaseSessionWithClientServer.
func (d DefaultSessionBuilder) NewSession(ctx context.Context, id uint32, conn *Connector) (sql.Session, error) {
	return memory.NewSession(sql.NewBaseSession(), d.provider), nil
}
