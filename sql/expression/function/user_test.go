package function

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/liquidata-inc/go-mysql-server/sql"
)

func TestUser(t *testing.T) {
	userFunc := sql.NewFunction0("user", sql.LongText, userFuncLogic)
	fn := userFunc.Fn

	session := sql.NewSession("server", "client", "root", 0)
	ctx := sql.NewContext(context.TODO(), sql.WithSession(session))

	user, err := fn().Eval(ctx, nil)
	require.NoError(t, err)
	assert.Equal(t, "root", user)

	session = sql.NewSession("server", "client", "someguy", 0)
	ctx = sql.NewContext(context.TODO(), sql.WithSession(session))

	user, err = fn().Eval(ctx, nil)
	require.NoError(t, err)
	assert.Equal(t, "someguy", user)

	ctx = sql.NewEmptyContext()

	user, err = fn().Eval(ctx, nil)
	require.NoError(t, err)
	assert.Equal(t, "", user)
}
