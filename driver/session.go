package driver

import (
	"context"
	"fmt"

	"github.com/dolthub/go-mysql-server/sql"
)

// A SessionBuilder creates SQL sessions.
type SessionBuilder interface {
	NewSession(ctx context.Context, id uint32, conn *Connector) (sql.Session, error)
}

// DefaultSessionBuilder creates basic SQL sessions.
type DefaultSessionBuilder struct{}

// NewSession calls sql.NewSession.
func (DefaultSessionBuilder) NewSession(ctx context.Context, id uint32, conn *Connector) (sql.Session, error) {
	return sql.NewSession(conn.Server(), sql.Client{Address: fmt.Sprintf("#%d", id)}, id), nil
}
