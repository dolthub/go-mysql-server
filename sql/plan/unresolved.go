package plan

import (
	"fmt"

	"github.com/mvader/gitql/sql"
)

type UnresolvedRelation struct {
	Name string
}

func (UnresolvedRelation) Resolved() bool {
	return false
}

func (UnresolvedRelation) Children() []sql.Node {
	return []sql.Node{}
}

func (UnresolvedRelation) Schema() sql.Schema {
	return sql.Schema{}
}

func (UnresolvedRelation) RowIter() (sql.RowIter, error) {
	return nil, fmt.Errorf("unresolved relation")
}
