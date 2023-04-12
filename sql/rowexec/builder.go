package rowexec

import (
	"github.com/dolthub/go-mysql-server/sql"
)

type builder struct{}

var DefaultBuilder = &builder{}

var _ sql.NodeExecBuilder = (*builder)(nil)

func (b *builder) Build(ctx *sql.Context, n sql.Node, r sql.Row) (sql.RowIter, error) {
	return b.buildNodeExec(ctx, n, r)
}
