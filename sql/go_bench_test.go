package sql

import (
	"testing"
)

var result_ Row

func BenchmarkCopyDefaultRow(b *testing.B) {
	var res UntypedSqlRow
	var r UntypedSqlRow = make([]interface{}, 1)
	for i := 0; i < b.N; i++ {
		res = r.Copy().(UntypedSqlRow)
	}
	result_ = Row(res)
}

func BenchmarkCopyRow(b *testing.B) {
	var res Row
	var r UntypedSqlRow = make([]interface{}, 1)
	for i := 0; i < b.N; i++ {
		res = r.Copy()
	}
	result_ = res
}
