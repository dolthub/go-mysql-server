package plan

import "github.com/mvader/gitql/sql"

type UnresolvedRelation struct {
	Name string
}

func (r *UnresolvedRelation) Schema() sql.Schema {

}
