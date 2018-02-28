package expression

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
)

func TestNot(t *testing.T) {
	require := require.New(t)

	e := NewNot(NewGetField(0, sql.Text, "foo", true))
	require.False(eval(t, e, sql.NewRow(true)).(bool))
	require.True(eval(t, e, sql.NewRow(false)).(bool))
	require.Nil(eval(t, e, sql.NewRow(nil)))
}
