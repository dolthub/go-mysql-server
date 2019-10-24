package parse

import (
	"strings"
	"testing"

	"github.com/src-d/go-mysql-server/sql"
	"github.com/src-d/go-mysql-server/sql/expression"
	"github.com/src-d/go-mysql-server/sql/plan"
	"github.com/stretchr/testify/require"
)

func TestParseCreateView(t *testing.T) {
	var fixtures = map[string]sql.Node{
		`CREATE VIEW myview AS SELECT 1`: plan.NewCreateView(
			sql.UnresolvedDatabase(""),
			"myview",
			nil,
			plan.NewSubqueryAlias("myview",
				plan.NewProject(
					[]sql.Expression{expression.NewLiteral(int8(1), sql.Int8)},
					plan.NewUnresolvedTable("dual", ""),
				),
			),
			false,
		),
		`CREATE VIEW myview AS SELECT * FROM mytable`: plan.NewCreateView(
			sql.UnresolvedDatabase(""),
			"myview",
			nil,
			plan.NewSubqueryAlias("myview",
				plan.NewProject(
					[]sql.Expression{expression.NewStar()},
					plan.NewUnresolvedTable("mytable", ""),
				),
			),
			false,
		),
		`CREATE VIEW mydb.myview AS SELECT 1`: plan.NewCreateView(
			sql.UnresolvedDatabase("mydb"),
			"myview",
			nil,
			plan.NewSubqueryAlias("myview",
				plan.NewProject(
					[]sql.Expression{expression.NewLiteral(int8(1), sql.Int8)},
					plan.NewUnresolvedTable("dual", ""),
				),
			),
			false,
		),
		`CREATE OR REPLACE VIEW mydb.myview AS SELECT 1`: plan.NewCreateView(
			sql.UnresolvedDatabase("mydb"),
			"myview",
			nil,
			plan.NewSubqueryAlias("myview",
				plan.NewProject(
					[]sql.Expression{expression.NewLiteral(int8(1), sql.Int8)},
					plan.NewUnresolvedTable("dual", ""),
				),
			),
			true,
		),
	}

	for query, expectedPlan := range fixtures {
		t.Run(query, func(t *testing.T) {
			require := require.New(t)

			ctx := sql.NewEmptyContext()
			lowerquery := strings.ToLower(query)
			result, err := parseCreateView(ctx, lowerquery)

			require.NoError(err)
			require.Equal(expectedPlan, result)
		})
	}
}
