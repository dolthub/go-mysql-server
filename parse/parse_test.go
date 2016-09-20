package parse

import (
	"strings"
	"testing"

	"github.com/mvader/gitql/sql"
	"github.com/mvader/gitql/sql/expression"
	"github.com/stretchr/testify/require"
)

const testSelect = `SELECT foo, bar FROM foo WHERE foo = bar;`

func TestParseSelectFromWhere(t *testing.T) {
	p := newParser(strings.NewReader(testSelect))
	require.Nil(t, p.parse())

	require.Equal(t, p.projection, []sql.Expression{
		expression.NewIdentifier("foo"),
		expression.NewIdentifier("bar"),
	})

	require.Equal(t, p.relation, "foo")

	require.Equal(t, p.filterClauses, []sql.Expression{
		expression.NewEquals(
			expression.NewIdentifier("foo"),
			expression.NewIdentifier("bar"),
		),
	})

	require.Nil(t, p.sortFields)
	require.Nil(t, p.err)
	require.Equal(t, DoneState, p.stateStack.pop())
}
