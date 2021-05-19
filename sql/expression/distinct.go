package expression

import (
	"fmt"

	"github.com/mitchellh/hashstructure"

	"github.com/dolthub/go-mysql-server/sql"
)

type DistinctExpression struct {
	seen    sql.KeyValueCache
	dispose sql.DisposeFunc
	Column  sql.Expression
}

func NewDistinctExpression(e sql.Expression) *DistinctExpression {
	return &DistinctExpression{
		Column: e,
	}
}

func (ad *DistinctExpression) shouldProcess(ctx *sql.Context, value interface{}) (bool, error) {
	if ad.seen == nil {
		cache, dispose := ctx.Memory.NewHistoryCache()
		ad.seen = cache
		ad.dispose = dispose
	}

	hash, err := hashstructure.Hash(value, nil)
	if err != nil {
		return false, err
	}

	if _, err := ad.seen.Get(hash); err == nil {
		return false, nil
	}

	if err := ad.seen.Put(hash, struct{}{}); err != nil {
		return false, err
	}

	return true, nil
}

func (ad *DistinctExpression) Dispose() {
	if ad.dispose != nil {
		ad.dispose()
	}

	ad.dispose = nil
	ad.seen = nil
}

func (ad *DistinctExpression) Resolved() bool {
	return ad.Column.Resolved()
}

func (ad *DistinctExpression) String() string {
	return fmt.Sprintf("DISTINCT %s", ad.Column.String())
}

func (ad *DistinctExpression) Type() sql.Type {
	return ad.Column.Type()
}

func (ad *DistinctExpression) IsNullable() bool {
	return false
}

func (ad *DistinctExpression) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	val, err := ad.Column.Eval(ctx, row)
	if err != nil {
		return nil, err
	}

	should, err := ad.shouldProcess(ctx, val)
	if err != nil {
		return nil, err
	}

	if should {
		return val, nil
	}

	return nil, nil
}

func (ad *DistinctExpression) Children() []sql.Expression {
	return []sql.Expression{ad.Column}
}

func (ad *DistinctExpression) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 1 {
		return nil, fmt.Errorf("ADO has an invalidnumber of children")
	}

	return &DistinctExpression{
		seen:    nil,
		dispose: nil,
		Column:  children[0],
	}, nil
}
