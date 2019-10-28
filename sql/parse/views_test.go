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

func TestParseDropView(t *testing.T) {
	var fixtures = map[string]sql.Node{
		`DROP VIEW view1`: plan.NewDropView(
			[]sql.Node{plan.NewSingleDropView(sql.UnresolvedDatabase(""), "view1")},
			false,
		),
		`DROP VIEW view1, view2`: plan.NewDropView(
			[]sql.Node{
				plan.NewSingleDropView(sql.UnresolvedDatabase(""), "view1"),
				plan.NewSingleDropView(sql.UnresolvedDatabase(""), "view2"),
			},
			false,
		),
		`DROP VIEW db1.view1`: plan.NewDropView(
			[]sql.Node{
				plan.NewSingleDropView(sql.UnresolvedDatabase("db1"), "view1"),
			},
			false,
		),
		`DROP VIEW db1.view1, view2`: plan.NewDropView(
			[]sql.Node{
				plan.NewSingleDropView(sql.UnresolvedDatabase("db1"), "view1"),
				plan.NewSingleDropView(sql.UnresolvedDatabase(""), "view2"),
			},
			false,
		),
		`DROP VIEW view1, db2.view2`: plan.NewDropView(
			[]sql.Node{
				plan.NewSingleDropView(sql.UnresolvedDatabase(""), "view1"),
				plan.NewSingleDropView(sql.UnresolvedDatabase("db2"), "view2"),
			},
			false,
		),
		`DROP VIEW db1.view1, db2.view2`: plan.NewDropView(
			[]sql.Node{
				plan.NewSingleDropView(sql.UnresolvedDatabase("db1"), "view1"),
				plan.NewSingleDropView(sql.UnresolvedDatabase("db2"), "view2"),
			},
			false,
		),
		`DROP VIEW IF EXISTS myview`: plan.NewDropView(
			[]sql.Node{plan.NewSingleDropView(sql.UnresolvedDatabase(""), "myview")},
			true,
		),
		`DROP VIEW IF EXISTS db1.view1, db2.view2`: plan.NewDropView(
			[]sql.Node{
				plan.NewSingleDropView(sql.UnresolvedDatabase("db1"), "view1"),
				plan.NewSingleDropView(sql.UnresolvedDatabase("db2"), "view2"),
			},
			true,
		),
		`DROP VIEW IF EXISTS db1.view1, db2.view2 RESTRICT CASCADE`: plan.NewDropView(
			[]sql.Node{
				plan.NewSingleDropView(sql.UnresolvedDatabase("db1"), "view1"),
				plan.NewSingleDropView(sql.UnresolvedDatabase("db2"), "view2"),
			},
			true,
		),
	}

	for query, expectedPlan := range fixtures {
		t.Run(query, func(t *testing.T) {
			require := require.New(t)

			ctx := sql.NewEmptyContext()
			lowerquery := strings.ToLower(query)
			result, err := parseDropView(ctx, lowerquery)

			require.NoError(err)
			require.Equal(expectedPlan, result)
		})
	}
}
