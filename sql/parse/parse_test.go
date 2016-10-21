package parse

import (
	"strings"
	"testing"

	"github.com/mvader/gitql/sql"
	"github.com/mvader/gitql/sql/expression"
	"github.com/stretchr/testify/require"
)

const testSelectFromWhere = `SELECT foo, bar FROM foo WHERE foo = bar;`
const testSelectFrom = `SELECT foo, bar FROM foo;`

func TestParseSelectFromWhere(t *testing.T) {
	p := newParser(strings.NewReader(testSelectFromWhere))
	require.Nil(t, p.parse())

	require.Equal(t, p.projection, []sql.Expression{
		expression.NewUnresolvedColumn("foo"),
		expression.NewUnresolvedColumn("bar"),
	})

	require.Equal(t, p.relation, "foo")

	require.Equal(t, p.filterClauses, []sql.Expression{
		expression.NewEquals(
			expression.NewUnresolvedColumn("foo"),
			expression.NewUnresolvedColumn("bar"),
		),
	})

	require.Nil(t, p.sortFields)
	require.Nil(t, p.err)
	require.Equal(t, DoneState, p.stateStack.pop())
}

func TestParseSelectFrom(t *testing.T) {
	p := newParser(strings.NewReader(testSelectFrom))
	require.Nil(t, p.parse())

	require.Equal(t, p.projection, []sql.Expression{
		expression.NewUnresolvedColumn("foo"),
		expression.NewUnresolvedColumn("bar"),
	})

	require.Equal(t, p.relation, "foo")

	require.Nil(t, p.sortFields)
	require.Nil(t, p.err)
	require.Equal(t, DoneState, p.stateStack.pop())
}
