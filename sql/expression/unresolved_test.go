package expression

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/src-d/go-mysql-server/sql"
)

func TestUnresolvedExpression(t *testing.T) {
	require := require.New(t)
	var e sql.Expression = NewUnresolvedColumn("test_col")
	require.NotNil(e)
	var o sql.Expression = NewEquals(e, e)
	require.NotNil(o)
	o = NewNot(e)
	require.NotNil(o)
}
