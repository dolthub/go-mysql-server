package expression

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
)

func TestNot(t *testing.T) {
	require := require.New(t)

	e := NewNot(NewGetField(0, sql.Text, "foo", true))
	require.False(eval(t, e, sql.NewRow(true)).(bool))
	require.True(eval(t, e, sql.NewRow(false)).(bool))
	require.Nil(eval(t, e, sql.NewRow(nil)))
	require.False(eval(t, e, sql.NewRow(1)).(bool))
	require.True(eval(t, e, sql.NewRow(0)).(bool))
	require.False(eval(t, e, sql.NewRow(time.Now())).(bool))
	require.False(eval(t, e, sql.NewRow(time.Second)).(bool))
	require.True(eval(t, e, sql.NewRow("any string always false")).(bool))
}
