package memory

import (
	"fmt"
	"github.com/src-d/go-mysql-server/sql"
	"github.com/src-d/go-mysql-server/sql/expression"
	"io"
	"strings"
)

// A very dumb index that iterates over the rows of a table, evaluates its matching expressions against each row, and
// stores those values to be later retrieved. Only here to test the functionality of indexed queries. This kind of index
// cannot be merged with any other index.
type UnmergeableDummyIndex struct {
	DB         string // required for engine tests with driver
	DriverName string // required for engine tests with driver
	Tbl        *Table // required for engine tests with driver
	TableName  string
	Exprs      []sql.Expression
}

func (u *UnmergeableDummyIndex) Database() string { return u.DB }
func (u *UnmergeableDummyIndex) Driver() string   { return u.DriverName }

func (u *UnmergeableDummyIndex) Expressions() []string {
	var exprs []string
	for _, e := range u.Exprs {
		exprs = append(exprs, e.String())
	}
	return exprs
}

func (u *UnmergeableDummyIndex) Get(key ...interface{}) (sql.IndexLookup, error) {
	return &UnmergeableIndexLookup{
		key: key,
		idx: u,
	}, nil
}

type UnmergeableIndexLookup struct {
	key []interface{}
	idx *UnmergeableDummyIndex
}

type unmergeableIndexValueIter struct {
	tbl *Table
	partition sql.Partition
	// Returns a set of expresssions that must match a given row to be included in the value iterator for a lookup
	matchExpressions func() []sql.Expression
	values [][]byte
	i int
}

func (u *unmergeableIndexValueIter) Next() ([]byte, error) {
	err := u.initValues()
	if err != nil {
		return nil, err
	}

	if u.i < len(u.values) {
		valBytes := u.values[u.i]
		u.i++
		return valBytes, nil
	}

	return nil, io.EOF
}

func (u *unmergeableIndexValueIter) initValues() error {
	if u.values == nil {
		rows, ok := u.tbl.partitions[string(u.partition.Key())]
		if !ok {
			return fmt.Errorf(
				"partition not found: %q", u.partition.Key(),
			)
		}

		for i, row := range rows {
			match := true
			for _, matchExpr := range u.matchExpressions() {
				ok, err := sql.EvaluateCondition(sql.NewEmptyContext(), matchExpr, row)
				if err != nil {
					return err
				}

				if !ok {
					match = false
					break
				}
			}

			if match {
				idxVal := &indexValue{
					Pos: i,
				}
				encoded, err := encodeIndexValue(idxVal)
				if err != nil {
					return err
				}

				u.values = append(u.values, encoded)
			}
		}
	}

	return nil
}

func getType(val interface{}) (interface{}, sql.Type) {
	switch val := val.(type) {
	case int8:
		return int64(val), sql.Int64
	case uint8:
		return int64(val), sql.Int64
	case int16:
		return int64(val), sql.Int64
	case uint16:
		return int64(val), sql.Int64
	case int32:
		return int64(val), sql.Int64
	case uint32:
		return int64(val), sql.Int64
	case int64:
		return int64(val), sql.Int64
	case uint64:
		return int64(val), sql.Int64
	case float32:
		return float64(val), sql.Float64
	case float64:
		return float64(val), sql.Float64
	case string:
		return val, sql.Text
	default:panic(fmt.Sprintf("Unsupported type for %v of type %T", val, val))
	}
}

func (u *unmergeableIndexValueIter) Close() error {
	return nil
}

func (u *UnmergeableIndexLookup) Values(p sql.Partition) (sql.IndexValueIter, error) {
	return &unmergeableIndexValueIter{
		tbl:       u.idx.Tbl,
		partition: p,
		matchExpressions: func() []sql.Expression {
			var exprs []sql.Expression
			for exprI, expr := range u.idx.Exprs {
				lit, typ := getType(u.key[exprI])
				exprs = append(exprs, expression.NewEquals(expr, expression.NewLiteral(lit, typ)))
			}
			return exprs
	}}, nil
}

func (u *UnmergeableIndexLookup) Indexes() []string {
	var idxes = make([]string, len(u.key))
	for i, e := range u.idx.Exprs {
		idxes[i] = fmt.Sprint(e)
	}
	return idxes
}

func (u *UnmergeableDummyIndex) Has(partition sql.Partition, key ...interface{}) (bool, error) {
	panic("unimplemented")
}

func (u *UnmergeableDummyIndex) ID() string {
	if len(u.Exprs) == 1 {
		return u.Exprs[0].String()
	}
	var parts = make([]string, len(u.Exprs))
	for i, e := range u.Exprs {
		parts[i] = e.String()
	}

	return "(" + strings.Join(parts, ", ") + ")"
}

func (u *UnmergeableDummyIndex) Table() string {
	return u.TableName
}

