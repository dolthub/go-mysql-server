package norm

import (
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
)

type Factory struct {
}

func (f *Factory) buildJoin(n *plan.JoinNode) sql.Node {
	return nil
}

func (f *Factory) buildFilter(n *plan.JoinNode) sql.Node {
	return nil
}
