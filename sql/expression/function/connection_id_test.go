package function

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/liquidata-inc/go-mysql-server/sql"
)

func TestConnectionID(t *testing.T) {
	require := require.New(t)

	session := sql.NewSession("", "", "", 2)
	ctx := sql.NewContext(context.Background(), sql.WithSession(session))

	connIDFunc := sql.NewFunction0("connection_id", sql.Uint32, connIDFuncLogic)
	result, err := connIDFunc.Fn().Eval(ctx, nil)
	require.NoError(err)
	require.Equal(uint32(2), result)
}
