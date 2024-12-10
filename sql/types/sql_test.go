package types

import (
	"testing"

	"github.com/dolthub/vitess/go/sqltypes"
	"github.com/shopspring/decimal"

	"github.com/dolthub/go-mysql-server/sql"
)

var result_ sqltypes.Value

func BenchmarkNumI64SQL(b *testing.B) {
	var res sqltypes.Value
	t := Int64
	ctx := sql.NewEmptyContext()
	for i := 0; i < b.N; i++ {
		res, _ = t.SQL(ctx, nil, i)
	}
	result_ = res
}

func BenchmarkVarchar10SQL(b *testing.B) {
	var res sqltypes.Value
	t := MustCreateStringWithDefaults(sqltypes.VarChar, 10)
	ctx := sql.NewEmptyContext()
	for i := 0; i < b.N; i++ {
		res, _ = t.SQL(ctx, nil, "char")
	}
	result_ = res
}

func BenchmarkTimespanSQL(b *testing.B) {
	var res sqltypes.Value
	t := TimespanType_{}
	ctx := sql.NewEmptyContext()
	for i := 0; i < b.N; i++ {
		res, _ = t.SQL(ctx, nil, i%60)
	}
	result_ = res
}

func BenchmarkTimestampSQL(b *testing.B) {
	var res sqltypes.Value
	t := Timestamp
	ctx := sql.NewEmptyContext()
	for i := 0; i < b.N; i++ {
		res, _ = t.SQL(ctx, nil, "2019-12-31T12:00:00Z")
	}
	result_ = res
}

func BenchmarkDatetimeSQL(b *testing.B) {
	var res sqltypes.Value
	t := Datetime
	ctx := sql.NewEmptyContext()
	for i := 0; i < b.N; i++ {
		res, _ = t.SQL(ctx, nil, "2019-12-31T12:00:00Z")
	}
	result_ = res
}

func BenchmarkEnumSQL(b *testing.B) {
	var res sqltypes.Value
	t, _ := CreateEnumType([]string{"a", "b", "c"}, sql.Collation_Default)
	ctx := sql.NewEmptyContext()
	for i := 0; i < b.N; i++ {
		res, _ = t.SQL(ctx, nil, "a")
	}
	result_ = res
}

func BenchmarkSetSQL(b *testing.B) {
	var res sqltypes.Value
	t, _ := CreateSetType([]string{"a", "b", "c"}, sql.Collation_Default)
	ctx := sql.NewEmptyContext()
	for i := 0; i < b.N; i++ {
		res, _ = t.SQL(ctx, nil, "a")
	}
	result_ = res
}

func BenchmarkBitSQL(b *testing.B) {
	var res sqltypes.Value
	t := BitType_{numOfBits: 8}
	ctx := sql.NewEmptyContext()
	for i := 0; i < b.N; i++ {
		res, _ = t.SQL(ctx, nil, i%8)
	}
	result_ = res
}

func BenchmarkDecimalSQL(b *testing.B) {
	var res sqltypes.Value
	t, _ := CreateColumnDecimalType(2, 2)
	ctx := sql.NewEmptyContext()
	for i := 0; i < b.N; i++ {
		res, _ = t.SQL(ctx, nil, decimal.New(int64(i), 2))
	}
	result_ = res
}

func BenchmarkNumF64SQL(b *testing.B) {
	var res sqltypes.Value
	t := Float64
	ctx := sql.NewEmptyContext()
	for i := 0; i < b.N; i++ {
		res, _ = t.SQL(ctx, nil, i)
	}
	result_ = res
}
