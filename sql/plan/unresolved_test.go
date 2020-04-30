package plan

import (
	"testing"

	"github.com/liquidata-inc/go-mysql-server/sql"
	"github.com/stretchr/testify/require"
)

func TestUnresolvedTable(t *testing.T) {
	require := require.New(t)
	var n sql.Node = NewUnresolvedTable("test_table", "")
	require.NotNil(n)
}
