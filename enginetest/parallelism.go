package enginetest

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/enginetest/queries"
	"github.com/dolthub/go-mysql-server/enginetest/scriptgen/setup"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/transform"
)

func TestParallelismQueries(t *testing.T, harness Harness) {
	harness.Setup(setup.XySetup...)
	e := mustNewEngine(t, harness)
	defer e.Close()
	for _, tt := range queries.ParallelismTests {
		t.Run(tt.Query, func(t *testing.T) {
			evalParallelismTest(t, harness, e, tt.Query, tt.Parallel)
		})
	}
}

func evalParallelismTest(t *testing.T, harness Harness, e QueryEngine, query string, parallel bool) {
	ctx := NewContext(harness)
	ctx = ctx.WithQuery(query)
	a, err := analyzeQuery(ctx, e, query)
	require.NoError(t, err)
	require.Equal(t, parallel, findExchange(a), fmt.Sprintf("expected exchange: %t\nplan:\n%s", parallel, sql.DebugString(a)))
}

func findExchange(n sql.Node) bool {
	return transform.InspectUp(n, func(n sql.Node) bool {
		if n == nil {
			return false
		}
		_, ok := n.(*plan.Exchange)
		if ok {
			return true
		}

		if ex, ok := n.(sql.Expressioner); ok {
			for _, e := range ex.Expressions() {
				found := transform.InspectExpr(e, func(e sql.Expression) bool {
					sq, ok := e.(*plan.Subquery)
					if !ok {
						return false
					}
					return findExchange(sq.Query)
				})
				if found {
					return true
				}
			}
		}
		return false
	})
}
