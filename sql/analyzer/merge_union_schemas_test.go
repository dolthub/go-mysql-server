package analyzer

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/liquidata-inc/go-mysql-server/sql"
	"github.com/liquidata-inc/go-mysql-server/sql/expression"
	"github.com/liquidata-inc/go-mysql-server/sql/plan"
)

func TestMergeUnionSchemas(t *testing.T) {
	testCases := []struct {
		name string
		in   sql.Node
		out  sql.Node
		err  error
	}{
		{
			"Unresolved is unchanged",
			plan.NewUnion(
				plan.NewUnresolvedTable("mytable", ""),
				plan.NewUnresolvedTable("mytable", ""),
			),
			plan.NewUnion(
				plan.NewUnresolvedTable("mytable", ""),
				plan.NewUnresolvedTable("mytable", ""),
			),
			nil,
		},
		{
			"Matching Schemas is Unchanged",
			plan.NewUnion(
				plan.NewProject(
					[]sql.Expression{expression.NewLiteral(int64(1), sql.Int64)},
					plan.NewResolvedTable(dualTable),
				),
				plan.NewProject(
					[]sql.Expression{expression.NewLiteral(int64(3), sql.Int64)},
					plan.NewResolvedTable(dualTable),
				),
			),
			plan.NewUnion(
				plan.NewProject(
					[]sql.Expression{expression.NewLiteral(int64(1), sql.Int64)},
					plan.NewResolvedTable(dualTable),
				),
				plan.NewProject(
					[]sql.Expression{expression.NewLiteral(int64(3), sql.Int64)},
					plan.NewResolvedTable(dualTable),
				),
			),
			nil,
		},
		{
			"Mismatched Lengths is error",
			plan.NewUnion(
				plan.NewProject(
					[]sql.Expression{expression.NewLiteral(int64(1), sql.Int64)},
					plan.NewResolvedTable(dualTable),
				),
				plan.NewProject(
					[]sql.Expression{
						expression.NewLiteral(int64(3), sql.Int64),
						expression.NewLiteral(int64(6), sql.Int64),
					},
					plan.NewResolvedTable(dualTable),
				),
			),
			nil,
			errors.New("this is an error"),
		},
		{
			"Mismatched Types Coerced to Strings",
			plan.NewUnion(
				plan.NewProject(
					[]sql.Expression{expression.NewLiteral(int64(1), sql.Int64)},
					plan.NewResolvedTable(dualTable),
				),
				plan.NewProject(
					[]sql.Expression{expression.NewLiteral(int32(3), sql.Int32)},
					plan.NewResolvedTable(dualTable),
				),
			),
			plan.NewUnion(
				plan.NewProject(
					[]sql.Expression{
						expression.NewAlias(
							expression.NewConvert(
								expression.NewGetField(0, sql.Int64, "1", false), "char"), "1"),
					},
					plan.NewProject(
						[]sql.Expression{expression.NewLiteral(int64(1), sql.Int64)},
						plan.NewResolvedTable(dualTable),
					),
				),
				plan.NewProject(
					[]sql.Expression{
						expression.NewAlias(
							expression.NewConvert(
								expression.NewGetField(0, sql.Int32, "3", false), "char"), "3"),
					},
					plan.NewProject(
						[]sql.Expression{expression.NewLiteral(int32(3), sql.Int32)},
						plan.NewResolvedTable(dualTable),
					),
				),
			),
			nil,
		},
	}
	for _, c := range testCases {
		t.Run(c.name, func(t *testing.T) {
			require := require.New(t)
			out, err := mergeUnionSchemas(sql.NewEmptyContext(), nil, c.in, nil)
			if c.err == nil {
				require.NoError(err)
				require.NotNil(out)
				require.Equal(c.out, out)
			} else {
				require.Error(err)
			}
		})
	}
}
