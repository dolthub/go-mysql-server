package sql

import (
	"fmt"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFuncDeps_Project(t *testing.T) {
	t.Run("project const via equiv", func(t *testing.T) {
		{
			// a == b, a const, proj(b) => maintian const(b)
			fds := &FuncDepSet{all: cols(1, 2, 3)}
			fds.AddConstants(cols(1))
			fds.AddEquiv(1, 2)
			proj := NewProjectFDs(fds, cols(2), false)
			assert.Equal(t, "(2)", proj.Constants().String())
		}
	})
	t.Run("project pk via equiv", func(t *testing.T) {
		{
			// pk(a,b), a == c, proj(b,c) => maintain pk(c,b)
			fds := &FuncDepSet{all: cols(1, 2, 3)}
			fds.AddEquiv(1, 3)
			fds.AddStrictKey(cols(1, 2))
			proj := NewProjectFDs(fds, cols(2, 3), false)
			assert.Equal(t, "key(2,3)", proj.String())
		}

	})
	t.Run("distinct project adds strict key", func(t *testing.T) {
		fds := &FuncDepSet{all: cols(1, 2, 3)}
		fds.AddLaxKey(cols(1, 2, 3))
		proj := NewProjectFDs(fds, cols(1, 2, 3), true)
		assert.Equal(t, "key(1-3)", proj.String())
	})
	t.Run("columns preserved", func(t *testing.T) {
		// a == b, b == c, proj(a,c) maintains a == c
		fds := &FuncDepSet{all: cols(1, 2, 3)}
		fds.AddEquivSet(cols(1, 2, 3))
		proj := NewProjectFDs(fds, cols(1, 3), false)
		assert.Equal(t, "equiv(1,3)", proj.String())
	})
	t.Run("remove strict determinant constant", func(t *testing.T) {
		fds := &FuncDepSet{all: cols(1, 2, 3)}
		fds.AddConstants(cols(1))
		fds.AddStrictKey(cols(1, 2))
		proj := NewProjectFDs(fds, cols(2), false)
		assert.Equal(t, "key(2)", proj.String())
	})
	t.Run("remove lax determinant constant", func(t *testing.T) {
		fds := &FuncDepSet{all: cols(1, 2, 3)}
		fds.AddConstants(cols(1))
		fds.AddLaxKey(cols(1, 2))
		proj := NewProjectFDs(fds, cols(2), false)
		assert.Equal(t, "lax-key(2)", proj.String())
	})
}

func TestFuncDeps_CrossJoin(t *testing.T) {
	// create table abcde (a primary key, b int, c int not null, d int not null, e int not null)
	// create table mnpq (m primary key, n int, p int not null, q int not null)
	t.Run("cross product", func(t *testing.T) {
		abcde := &FuncDepSet{}
		abcde.AddNotNullable(cols(1))
		abcde.AddStrictKey(cols(1))
		abcde.AddLaxKey(cols(2, 3))

		mnpq := &FuncDepSet{}
		mnpq.AddNotNullable(cols(6, 7))
		mnpq.AddStrictKey(cols(6, 7))

		join := NewCrossJoinFDs(abcde, mnpq)
		assert.Equal(t, "key(1,6,7); lax-fd(2,3)", join.String())
	})
	t.Run("cross product one-sided equiv", func(t *testing.T) {
		abcde := &FuncDepSet{}
		abcde.AddNotNullable(cols(1))
		abcde.AddEquiv(1, 5)
		abcde.AddStrictKey(cols(1))
		abcde.AddLaxKey(cols(2, 3))

		mnpq := &FuncDepSet{}
		mnpq.AddNotNullable(cols(6, 7))
		mnpq.AddStrictKey(cols(6, 7))

		join := NewCrossJoinFDs(abcde, mnpq)
		assert.Equal(t, "key(1,6,7); equiv(1,5); lax-fd(2,3)", join.String())
	})
}

func TestFuncDeps_InnerJoin(t *testing.T) {
	t.Run("abcde X mnpq", func(t *testing.T) {
		// abcde JOIN mnpq ON a = m WHERE n = 2
		abcde := &FuncDepSet{all: cols(1, 2, 3, 4, 5)}
		abcde.AddNotNullable(cols(1))
		abcde.AddStrictKey(cols(1))
		abcde.AddLaxKey(cols(2, 3))

		mnpq := &FuncDepSet{all: cols(6, 7, 8, 9)}
		mnpq.AddNotNullable(cols(6, 7))
		mnpq.AddConstants(cols(7))
		mnpq.AddStrictKey(cols(6, 7))

		join := NewInnerJoinFDs(abcde, mnpq, [][2]ColumnId{{1, 6}})
		assert.Equal(t, "key(6); constant(7); equiv(1,6); lax-fd(2,3)", join.String())
	})

	t.Run("ware X cust", func(t *testing.T) {
		// create table customer (id primary key, did not null, wid int not null, first varchar(10), last varchar(10))
		// create table warehouse (id primary key)

		// c.wid = w.id
		// SELECT * from cust join ware
		// ON c_w_id = w_id AND
		// WHERE w_id = 1 AND c_d_id = 2 AND c_id = 2327

		cust := &FuncDepSet{all: cols(1, 2, 3, 4, 5)}
		cust.AddNotNullable(cols(1, 2, 3))
		cust.AddConstants(cols(1, 2))
		cust.AddStrictKey(cols(3, 2, 1))
		cust.AddLaxKey(cols(3, 2, 4, 5))

		ware := &FuncDepSet{all: cols(6)}
		ware.AddNotNullable(cols(6))
		ware.AddConstants(cols(6))
		ware.AddStrictKey(cols(6))

		join := NewInnerJoinFDs(cust, ware, [][2]ColumnId{{3, 6}})
		assert.Equal(t, "key(); constant(1-3,6); equiv(3,6)", join.String())
	})
	t.Run("equiv on both sides inner join", func(t *testing.T) {
		abcde := &FuncDepSet{all: cols(1, 2, 3, 4, 5)}
		abcde.AddNotNullable(cols(1))
		abcde.AddEquivSet(cols(2, 3, 4))
		abcde.AddStrictKey(cols(1))
		abcde.AddLaxKey(cols(2, 3))

		mnpq := &FuncDepSet{all: cols(6, 7, 8, 9)}
		mnpq.AddNotNullable(cols(6, 7))
		mnpq.AddEquivSet(cols(6, 8, 9))
		mnpq.AddStrictKey(cols(6, 7))

		join := NewInnerJoinFDs(mnpq, abcde, [][2]ColumnId{})
		assert.Equal(t, "key(1,6,7); equiv(6,8,9); equiv(2-4); lax-fd(3)", join.String())
	})
	t.Run("max1Row inner join", func(t *testing.T) {
		abcde := &FuncDepSet{all: cols(1, 2, 3, 4, 5)}
		abcde.AddNotNullable(cols(1, 2, 3))
		abcde.AddConstants(cols(3))
		abcde.AddEquiv(2, 3)
		abcde.AddStrictKey(cols(1))
		abcde.AddLaxKey(cols(2, 3))

		mnpq := &FuncDepSet{all: cols(6, 7, 8, 9)}
		mnpq.AddNotNullable(cols(6, 7))
		mnpq.AddConstants(cols(6, 7))
		mnpq.AddStrictKey(cols(6, 7))

		join := NewInnerJoinFDs(mnpq, abcde, [][2]ColumnId{{1, 6}, {1, 2}})
		assert.Equal(t, "key(); constant(1-9); equiv(1-3,6)", join.String())
	})
	t.Run("infer constants from max1Row", func(t *testing.T) {
		abcde := &FuncDepSet{all: cols(1, 2, 3, 4, 5)}
		abcde.AddNotNullable(cols(1, 2, 3))
		abcde.AddConstants(cols(1))
		abcde.AddStrictKey(cols(1))

		mnpq := &FuncDepSet{all: cols(6, 7, 8, 9)}
		mnpq.AddNotNullable(cols(6))
		mnpq.AddStrictKey(cols(6))

		join := NewInnerJoinFDs(mnpq, abcde, [][2]ColumnId{{1, 7}})
		assert.Equal(t, "key(6); constant(1-5,7); equiv(1,7)", join.String())
	})
}

func TestFuncDeps_LeftJoin(t *testing.T) {
	t.Run("preserved-side constants kept", func(t *testing.T) {
		abcde := &FuncDepSet{all: cols(1, 2, 3, 4, 5)}
		abcde.AddNotNullable(cols(1))
		abcde.AddStrictKey(cols(1))
		abcde.AddLaxKey(cols(2, 3))

		mnpq := &FuncDepSet{all: cols(6, 7, 8, 9)}
		mnpq.AddNotNullable(cols(6, 7))
		mnpq.AddConstants(cols(8, 9))
		mnpq.AddStrictKey(cols(6, 7))

		join := NewLeftJoinFDs(mnpq, abcde, [][2]ColumnId{})
		assert.Equal(t, "key(1,6,7); constant(8,9); lax-fd(2,3)", join.String())
	})
	t.Run("preserved-side equiv constants kept", func(t *testing.T) {
		abcde := &FuncDepSet{all: cols(1, 2, 3, 4, 5)}
		abcde.AddNotNullable(cols(1))
		abcde.AddStrictKey(cols(1))
		abcde.AddLaxKey(cols(2, 3))

		mnpq := &FuncDepSet{all: cols(6, 7, 8, 9)}
		mnpq.AddNotNullable(cols(6, 7))
		mnpq.AddEquiv(8, 9)
		mnpq.AddConstants(cols(8))
		mnpq.AddStrictKey(cols(6, 7))

		join := NewLeftJoinFDs(mnpq, abcde, [][2]ColumnId{})
		assert.Equal(t, "key(1,6,7); constant(8,9); equiv(8,9); lax-fd(2,3)", join.String())
	})
	t.Run("preserved-side key constants kept", func(t *testing.T) {
		abcde := &FuncDepSet{all: cols(1, 2, 3, 4, 5)}
		abcde.AddNotNullable(cols(1))
		abcde.AddStrictKey(cols(1))
		abcde.AddLaxKey(cols(2, 3))

		mnpq := &FuncDepSet{all: cols(6, 7, 8, 9)}
		mnpq.AddNotNullable(cols(6, 7))
		mnpq.AddConstants(cols(6, 8, 9))
		mnpq.AddStrictKey(cols(6, 7))

		join := NewLeftJoinFDs(mnpq, abcde, [][2]ColumnId{})
		assert.Equal(t, "key(1,7); constant(6,8,9); lax-fd(2,3)", join.String())
	})
	t.Run("null-project side constants removed", func(t *testing.T) {
		abcde := &FuncDepSet{all: cols(1, 2, 3, 4, 5)}
		abcde.AddNotNullable(cols(1))
		abcde.AddStrictKey(cols(1))
		abcde.AddConstants(cols(3, 4))
		abcde.AddLaxKey(cols(2, 3))

		mnpq := &FuncDepSet{all: cols(6, 7, 8, 9)}
		mnpq.AddNotNullable(cols(6, 7))
		mnpq.AddStrictKey(cols(6, 7))

		join := NewLeftJoinFDs(mnpq, abcde, [][2]ColumnId{})
		assert.Equal(t, "key(1,6,7); lax-fd(2)", join.String())
	})
	t.Run("equiv on both sides left join", func(t *testing.T) {
		abcde := &FuncDepSet{all: cols(1, 2, 3, 4, 5)}
		abcde.AddNotNullable(cols(1))
		abcde.AddEquivSet(cols(2, 3, 4))
		abcde.AddStrictKey(cols(1))
		abcde.AddLaxKey(cols(2, 3))

		mnpq := &FuncDepSet{all: cols(6, 7, 8, 9)}
		mnpq.AddNotNullable(cols(6, 7))
		mnpq.AddEquivSet(cols(6, 8, 9))
		mnpq.AddStrictKey(cols(6, 7))

		join := NewLeftJoinFDs(mnpq, abcde, [][2]ColumnId{})
		assert.Equal(t, "key(1,6,7); equiv(6,8,9); lax-fd(3)", join.String())
	})
	t.Run("join filter equiv", func(t *testing.T) {
		// SELECT * FROM abcde RIGHT OUTER JOIN mnpq ON a=m
		abcde := &FuncDepSet{all: cols(1, 2, 3, 4, 5)}
		abcde.AddNotNullable(cols(1))
		abcde.AddStrictKey(cols(1))
		abcde.AddLaxKey(cols(2, 3))

		mnpq := &FuncDepSet{all: cols(6, 7, 8, 9)}
		mnpq.AddNotNullable(cols(6, 7))
		mnpq.AddStrictKey(cols(6, 7))

		join := NewLeftJoinFDs(mnpq, abcde, [][2]ColumnId{{1, 6}})
		assert.Equal(t, "key(6,7); fd(1); lax-fd(2,3)", join.String())
	})
	t.Run("join filter equiv and null-side rel equiv", func(t *testing.T) {
		//   SELECT * FROM abcde RIGHT OUTER JOIN mnpq ON a=m AND a=b
		abcde := &FuncDepSet{all: cols(1, 2, 3, 4, 5)}
		abcde.AddNotNullable(cols(1))
		abcde.AddStrictKey(cols(1))
		abcde.AddLaxKey(cols(2, 3))

		mnpq := &FuncDepSet{all: cols(6, 7, 8, 9)}
		mnpq.AddNotNullable(cols(6, 7))
		mnpq.AddStrictKey(cols(6, 7))

		join := NewLeftJoinFDs(mnpq, abcde, [][2]ColumnId{{1, 6}, {1, 2}})
		assert.Equal(t, "key(6,7); fd(1); lax-fd(2,3)", join.String())
	})
	t.Run("max1Row left join", func(t *testing.T) {
		abcde := &FuncDepSet{all: cols(1, 2, 3, 4, 5)}
		abcde.AddNotNullable(cols(1, 2, 3))
		abcde.AddConstants(cols(3))
		abcde.AddEquiv(2, 3)
		abcde.AddStrictKey(cols(1))
		abcde.AddLaxKey(cols(2, 3))

		mnpq := &FuncDepSet{all: cols(6, 7, 8, 9)}
		mnpq.AddNotNullable(cols(6, 7))
		mnpq.AddConstants(cols(6, 7))
		mnpq.AddStrictKey(cols(6, 7))

		join := NewLeftJoinFDs(mnpq, abcde, [][2]ColumnId{{1, 6}, {1, 2}})
		assert.Equal(t, "key(); constant(1,6,7)", join.String())
	})
}

func TestEquivSets(t *testing.T) {
	tests := []struct {
		name string
		sets []ColSet
		exp  EquivSets
	}{
		{
			name: "all overlap",
			sets: []ColSet{
				cols(1, 2),
				cols(2, 3),
				cols(3, 4),
			},
			exp: EquivSets{sets: []ColSet{cols(1, 2, 3, 4)}},
		},
		{
			name: "no overlap",
			sets: []ColSet{
				cols(1, 2),
				cols(3, 4),
				cols(5, 6),
			},
			exp: EquivSets{sets: []ColSet{cols(1, 2), cols(3, 4), cols(5, 6)}},
		},
		{
			name: "add merges two previous sets",
			sets: []ColSet{
				cols(1, 2),
				cols(3, 4),
				cols(2, 3),
			},
			exp: EquivSets{sets: []ColSet{cols(1, 2, 3, 4)}},
		},
		{
			name: "add merges one previous set",
			sets: []ColSet{
				cols(1, 2),
				cols(3, 4),
				cols(2, 6),
			},
			exp: EquivSets{sets: []ColSet{cols(1, 2, 6), cols(3, 4)}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			equiv := EquivSets{}
			for _, set := range tt.sets {
				equiv.Add(set)
			}
			sort.Slice(equiv.sets, func(i, j int) bool {
				return equiv.sets[i].set.String() < equiv.sets[j].set.String()
			})
			assert.Equal(t, tt.exp, equiv, fmt.Sprintf("exp != found:\n  [%s]\n  [%s]", tt.exp.String(), equiv.String()))
		})
	}
}

func cols(vals ...ColumnId) ColSet {
	return NewColSet(vals...)
}
