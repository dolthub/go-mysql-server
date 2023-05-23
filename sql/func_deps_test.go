package sql

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

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

func TestFuncDeps_A(t *testing.T) {
	// create table customer (id primary key, wid int not null, did int not null, first varchar(10), last varchar(10))
	// create table warehouse (id primary key)
	cust := &FuncDepsSet{}
	cust.AddNotNullable(cols(1, 2, 3))
	// w.id = 1, c.did = 2, c.id = 2327
	cust.AddConstants(cols(1, 3))
	cust.AddStrictKey(cols(1))
	cust.AddLaxKey(cols(2, 3))

	ware := &FuncDepsSet{}
	ware.AddConstants(cols(6))
	ware.AddNotNullable(cols(6))
	ware.AddStrictKey(cols(6))

	// c.wid = w.id
	ware.AddEquiv(2, 6)
}

func TestFuncDeps_CrossJoin(t *testing.T) {
	// create table abcde (a primary key, b int, c int not null, d int not null, e int not null)
	// create table mnpq (m primary key, n int, p int not null, q int not null)
	abcde := &FuncDepsSet{}
	abcde.AddNotNullable(cols(1))
	abcde.AddStrictKey(cols(1))
	abcde.AddLaxKey(cols(2, 3))

	mnpq := &FuncDepsSet{}
	mnpq.AddNotNullable(cols(6, 7))
	mnpq.AddStrictKey(cols(6, 7))

	join := NewCrossJoinFDs(abcde, mnpq)
	assert.Equal(t, "key(1,6,7); lax-key(2,3)", join.String())

}

func TestFuncDeps_InnerJoin(t *testing.T) {
	t.Run("abcde X mnpq", func(t *testing.T) {
		// abcde JOIN mnpq ON a = m WHERE n = 2
		abcde := &FuncDepsSet{}
		abcde.AddNotNullable(cols(1))
		abcde.AddStrictKey(cols(1))
		abcde.AddLaxKey(cols(2, 3))

		mnpq := &FuncDepsSet{}
		mnpq.AddNotNullable(cols(6, 7))
		mnpq.AddConstants(cols(7))
		mnpq.AddStrictKey(cols(6, 7))

		join := NewInnerJoinFDs(abcde, mnpq, [][2]ColumnId{{1, 6}})
		assert.Equal(t, "key(6); constant(7); equiv(1,6); lax-key(2,3)", join.String())
	})

	t.Run("ware X cust", func(t *testing.T) {
		// create table customer (id primary key, did not null, wid int not null, first varchar(10), last varchar(10))
		// create table warehouse (id primary key)

		// c.wid = w.id
		// SELECT * from cust join ware
		// ON c_w_id = w_id AND
		// WHERE w_id = 1 AND c_d_id = 2 AND c_id = 2327

		cust := &FuncDepsSet{}
		cust.AddNotNullable(cols(1, 2, 3))
		cust.AddConstants(cols(1, 2))
		cust.AddStrictKey(cols(3, 2, 1))
		cust.AddLaxKey(cols(3, 2, 4, 5))

		ware := &FuncDepsSet{}
		ware.AddNotNullable(cols(6))
		ware.AddConstants(cols(6))
		ware.AddStrictKey(cols(6))

		join := NewInnerJoinFDs(cust, ware, [][2]ColumnId{{3, 6}})
		assert.Equal(t, "key(); constant(1-3,6); equiv(3,6); lax-key(4,5)", join.String())
	})
}

func TestFuncDeps_Project(t *testing.T) {
	t.Run("project const via equiv", func(t *testing.T) {
		{
			// a == b, a const, proj(b) => maintian const(b)
			fds := &FuncDepsSet{}
			fds.AddConstants(cols(1))
			fds.AddEquiv(1, 2)
			proj := NewProjectFDs(fds, cols(2), false)
			assert.Equal(t, "(2)", proj.Constants().String())
		}
	})
	t.Run("project pk via equiv", func(t *testing.T) {
		{
			// pk(a,b), a == c, prok(b,c) => maintain pk(c,b)
			fds := &FuncDepsSet{}
			fds.AddEquiv(1, 3)
			fds.AddStrictKey(cols(1, 2))
			proj := NewProjectFDs(fds, cols(2, 3), false)
			assert.Equal(t, "key(2,3)", proj.String())
		}

	})
	t.Run("distinct project adds strict key", func(t *testing.T) {

	})
	t.Run("columns preserved", func(t *testing.T) {
		// a == b, b == c, proj(a,c) maintains a == c
	})
}

func TestFuncDeps_LeftJoin(t *testing.T) {

}

func cols(vals ...ColumnId) ColSet {
	return NewColSet(vals...)
}
