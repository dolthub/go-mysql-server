package rowexec

import (
	"github.com/dolthub/go-mysql-server/sql"
)

type builder struct{}

func (b *builder) Build(ctx *sql.Context, n sql.Node) (sql.RowIter, error) {
	return b.buildNodeExec(ctx, n, nil)
}
