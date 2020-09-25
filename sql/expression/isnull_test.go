package expression

import (
	"testing"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/stretchr/testify/require"
)

func TestIsNull(t *testing.T) {
	require := require.New(t)

	get0 := NewGetField(0, sql.Text, "col1", true)
	e := NewIsNull(get0)
	require.Equal(sql.Boolean, e.Type())
	require.Equal(false, e.IsNullable())
	require.Equal(true, eval(t, e, sql.NewRow(nil)))
	require.Equal(false, eval(t, e, sql.NewRow("")))
}
