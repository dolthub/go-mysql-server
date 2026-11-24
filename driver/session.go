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
	provider sql.DatabaseProvider
}

func NewDefaultSessionBuilder(provider sql.DatabaseProvider) *DefaultSessionBuilder {
	return &DefaultSessionBuilder{
		provider: provider,
	}
}

// NewSession constructs a session from the connector's engine state.
func (d DefaultSessionBuilder) NewSession(ctx context.Context, id uint32, conn *Connector) (sql.Session, error) {
	return memory.NewSession(sql.NewBaseSessionWithVariables(conn.EngineVariables()), d.provider), nil
}
