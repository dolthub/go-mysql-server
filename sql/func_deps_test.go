package sql

import "testing"

// tables: customer, warehouse
// warehouse: id
// customer: id, wid, did
//
// select * from customer c
// join warehouse w
// on w.id = c.wid
// where
//   w.id = 1 and
//   c.did = 2 and
//   c.id = 2327
//
// need:
//  - add table -> pk
//  - add constant filter
//  - is not null filters
//  - join condition
//
// add warehouse (keys)
//  - (id)
// add customer (keys)
//  - (wid, did, id)
//  - (wid, did, lastname, firstname)
// add join (join conditions)
//  - w.id = c.wid
// add constant (remove determinants)
//  - c.id = 2327 => (customer pk to (wid, did))
//  - c.did = 2 => (customer pk to (wid))
//  - w.id = 1 => (equivalence => c.wid = 1), warehouse pk to ())
//  - (eq) c.wid = 1 => (customer pk to ())
//
// prereq:
//  - columnId in memo

type testFdProp uint16

const (
	fdTablescan testFdProp = iota + 1
	fdInnerJoin
	fdEq
)

type testFdOp struct {
	prop testFdProp
	args []ColumnId
}

func TestFuncDeps(t *testing.T) {

}
