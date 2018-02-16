package expression

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
)

func TestStar(t *testing.T) {
	require := require.New(t)
	var e sql.Expression = NewStar()
	require.NotNil(e)
	require.Equal("*", e.Name())
}
