package auth

import (
	"github.com/src-d/go-mysql-server/sql"

	"gopkg.in/src-d/go-vitess.v1/mysql"
)

// None is an Auth method that always succeeds.
type None struct{}

// Mysql implements Auth interface.
func (n *None) Mysql() mysql.AuthServer {
	return new(mysql.AuthServerNone)
}

// Mysql implements Auth interface.
func (n *None) Allowed(ctx *sql.Context, permission Permission) error {
	return nil
}
