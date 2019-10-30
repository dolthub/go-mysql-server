package memory

import (
	"fmt"
	"github.com/src-d/go-mysql-server/sql"
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
	return &UnmergeableIndexLookup{key: key}, nil
}

type UnmergeableIndexLookup struct {
	key []interface{}
	idx UnmergeableDummyIndex
}

type unmergeableIndexValueIter struct {
	tbl *Table
	partition sql.Partition
	lookup *UnmergeableIndexLookup
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
			for exprI, expr := range u.lookup.idx.Exprs {
				colVal, err := expr.Eval(sql.NewEmptyContext(), row)
				if colVal != u.lookup.key[exprI] {
					match = false
					break
				}

				if err != nil {
					return err
				}
			}

			if match {
				idxVal := &indexValue{
					Key: "",
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

func (u *unmergeableIndexValueIter) Close() error {
	return nil
}

func (u *UnmergeableIndexLookup) Values(p sql.Partition) (sql.IndexValueIter, error) {
	return &unmergeableIndexValueIter{
		tbl:       u.idx.Tbl,
		partition: p,
		lookup:    u,
	}, nil
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

