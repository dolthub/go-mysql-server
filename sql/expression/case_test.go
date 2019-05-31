package expression

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/src-d/go-mysql-server/sql"
)

func TestCase(t *testing.T) {
	f1 := NewCase(
		NewGetField(0, sql.Int64, "foo", false),
		[]CaseBranch{
			{Cond: NewLiteral(int64(1), sql.Int64), Value: NewLiteral(int64(2), sql.Int64)},
			{Cond: NewLiteral(int64(3), sql.Int64), Value: NewLiteral(int64(4), sql.Int64)},
			{Cond: NewLiteral(int64(5), sql.Int64), Value: NewLiteral(int64(6), sql.Int64)},
		},
		NewLiteral(int64(7), sql.Int64),
	)

	f2 := NewCase(
		nil,
		[]CaseBranch{
			{
				Cond: NewEquals(
					NewGetField(0, sql.Int64, "foo", false),
					NewLiteral(int64(1), sql.Int64),
				),
				Value: NewLiteral(int64(2), sql.Int64),
			},
			{
				Cond: NewEquals(
					NewGetField(0, sql.Int64, "foo", false),
					NewLiteral(int64(3), sql.Int64),
				),
				Value: NewLiteral(int64(4), sql.Int64),
			},
			{
				Cond: NewEquals(
					NewGetField(0, sql.Int64, "foo", false),
					NewLiteral(int64(5), sql.Int64),
				),
				Value: NewLiteral(int64(6), sql.Int64),
			},
		},
		NewLiteral(int64(7), sql.Int64),
	)

	f3 := NewCase(
		NewGetField(0, sql.Int64, "foo", false),
		[]CaseBranch{
			{Cond: NewLiteral(int64(1), sql.Int64), Value: NewLiteral(int64(2), sql.Int64)},
			{Cond: NewLiteral(int64(3), sql.Int64), Value: NewLiteral(int64(4), sql.Int64)},
			{Cond: NewLiteral(int64(5), sql.Int64), Value: NewLiteral(int64(6), sql.Int64)},
		},
		nil,
	)

	testCases := []struct {
		name     string
		f        *Case
		row      sql.Row
		expected interface{}
	}{
		{
			"with expr and else branch 1",
			f1,
			sql.Row{int64(1)},
			int64(2),
		},
		{
			"with expr and else branch 2",
			f1,
			sql.Row{int64(3)},
			int64(4),
		},
		{
			"with expr and else branch 3",
			f1,
			sql.Row{int64(5)},
			int64(6),
		},
		{
			"with expr and else, else branch",
			f1,
			sql.Row{int64(9)},
			int64(7),
		},
		{
			"without expr and else branch 1",
			f2,
			sql.Row{int64(1)},
			int64(2),
		},
		{
			"without expr and else branch 2",
			f2,
			sql.Row{int64(3)},
			int64(4),
		},
		{
			"without expr and else branch 3",
			f2,
			sql.Row{int64(5)},
			int64(6),
		},
		{
			"without expr and else, else branch",
			f2,
			sql.Row{int64(9)},
			int64(7),
		},
		{
			"without else, else branch",
			f3,
			sql.Row{int64(9)},
			nil,
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)
			result, err := tt.f.Eval(sql.NewEmptyContext(), tt.row)
			require.NoError(err)
			require.Equal(tt.expected, result)
		})
	}
}
