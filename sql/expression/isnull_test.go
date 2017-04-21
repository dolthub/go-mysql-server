package expression

import (
	"testing"

	"gopkg.in/sqle/sqle.v0/sql"

	"github.com/stretchr/testify/require"
)

func TestIsNull(t *testing.T) {
	require := require.New(t)

	get0 := NewGetField(0, sql.String, "col1", true)
	e := NewIsNull(get0)
	require.Equal(sql.Boolean, e.Type())
	require.Equal(false, e.IsNullable())
	require.Equal(true, e.Eval(sql.NewRow(nil)))
	require.Equal(false, e.Eval(sql.NewRow("")))
}
