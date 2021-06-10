package expression

import (
	"fmt"

	"github.com/mitchellh/hashstructure"

	"github.com/dolthub/go-mysql-server/sql"
)

type DistinctExpression struct {
	seen    sql.KeyValueCache
	dispose sql.DisposeFunc
	Child   sql.Expression
}

var _ sql.Disposable = (*DistinctExpression)(nil)

func NewDistinctExpression(e sql.Expression) *DistinctExpression {
	return &DistinctExpression{
		Child: e,
	}
}

func (de *DistinctExpression) seenValue(ctx *sql.Context, value interface{}) (bool, error) {
	if de.seen == nil {
		cache, dispose := ctx.Memory.NewHistoryCache()
		de.seen = cache
		de.dispose = dispose
	}

	hash, err := hashstructure.Hash(value, nil)
	if err != nil {
		return false, err
	}

	if _, err := de.seen.Get(hash); err == nil {
		return false, nil
	}

	if err := de.seen.Put(hash, struct{}{}); err != nil {
		return false, err
	}

	return true, nil
}

func (de *DistinctExpression) Dispose() {
	if de.dispose != nil {
		de.dispose()
	}

	de.dispose = nil
	de.seen = nil
}

func (de *DistinctExpression) Resolved() bool {
	return de.Child.Resolved()
}

func (de *DistinctExpression) String() string {
	return fmt.Sprintf("DISTINCT %s", de.Child.String())
}

func (de *DistinctExpression) Type() sql.Type {
	return de.Child.Type()
}

func (de *DistinctExpression) IsNullable() bool {
	return false
}

// Returns the child value if the cache hasn't seen the value before otherwise returns nil.
// Since NULLs are ignored in aggregate expressions that use DISTINCT this is a valid return scheme.
func (de *DistinctExpression) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	val, err := de.Child.Eval(ctx, row)
	if err != nil {
		return nil, err
	}

	should, err := de.seenValue(ctx, val)
	if err != nil {
		return nil, err
	}

	if should {
		return val, nil
	}

	return nil, nil
}

func (de *DistinctExpression) Children() []sql.Expression {
	return []sql.Expression{de.Child}
}

func (de *DistinctExpression) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 1 {
		return nil, fmt.Errorf("DistinctExpression has an invalid number of children")
	}

	return &DistinctExpression{
		seen:    nil,
		dispose: nil,
		Child:   children[0],
	}, nil
}
