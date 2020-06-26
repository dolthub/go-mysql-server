package function

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/liquidata-inc/go-mysql-server/sql"
)

const versionPostfix = "test"

func TestNewVersion(t *testing.T) {
	require := require.New(t)

	f, _ := NewVersion(versionPostfix)()
	ctx := sql.NewEmptyContext()

	val, err := f.Eval(ctx, nil)
	require.NoError(err)
	require.Equal("8.0.11-"+versionPostfix, val)

	f, _ = NewVersion("")()

	val, err = f.Eval(ctx, nil)
	require.NoError(err)
	require.Equal("8.0.11", val)
}
