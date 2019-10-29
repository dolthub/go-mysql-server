package testutil

import (
	"fmt"
	"github.com/src-d/go-mysql-server/sql"
	"strings"
)

type UnmergeableDummyIndex struct {
	DB         string // required for engine tests with driver
	DriverName string // required for engine tests with driver
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
	if len(key) != 1 {
		var parts = make([]string, len(key))
		for i, p := range key {
			parts[i] = fmt.Sprint(p)
		}

		return &UnmergeableIndexLookup{id: strings.Join(parts, ", ")}, nil
	}

	return &UnmergeableIndexLookup{id: fmt.Sprint(key[0])}, nil
}

type UnmergeableIndexLookup struct {
	id string
}

func (u UnmergeableIndexLookup) Values(sql.Partition) (sql.IndexValueIter, error) {
	return nil, nil
}

func (u UnmergeableIndexLookup) Indexes() []string {
	return []string{u.id}
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

