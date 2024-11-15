package sql

import (
	"fmt"
	"testing"
)

var result_ []interface{}

func BenchmarkSliceCopy(b *testing.B) {
	var res []interface{}

	r := []interface{}{1, 2, 3}
	for i := 0; i < b.N; i++ {
		res = make([]interface{}, len(r))
		copy(res, r)
	}
	result_ = res
}

func TestRowCopy2(t *testing.T) {
	r1 := UntypedSqlRow{1, 2, 3}
	r2 := r1.Copy()
	r1[0] = 4
	fmt.Println(r1, r2)
}

func BenchmarkRowCopy(b *testing.B) {
	var res Row

	r := UntypedSqlRow{1, 2, 3}
	for i := 0; i < b.N; i++ {
		res = r.Copy()
	}
	result_ = res.Values()
}

func BenchmarkUntypedRowCopyToRow(b *testing.B) {
	var res Row
	r := UntypedSqlRow{1, 2, 3}
	for i := 0; i < b.N; i++ {
		r2 := make(UntypedSqlRow, len(r))
		copy(r2, r)
		res = r2
	}
	result_ = res.Values()
}

func BenchmarkUntypedRowCopy(b *testing.B) {
	var res UntypedSqlRow
	r := UntypedSqlRow{1, 2, 3}
	for i := 0; i < b.N; i++ {
		r2 := make(UntypedSqlRow, len(r))
		copy(r2, r)
		res = r2
	}
	result_ = res.Values()
}
