package auth

import (
	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/vitess/go/mysql"
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
