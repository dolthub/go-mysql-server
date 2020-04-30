package function

import (
	"context"
	"github.com/liquidata-inc/go-mysql-server/sql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestUser(t *testing.T) {
	fn := NewUser()

	session := sql.NewSession("server", "client", "root", 0)
	ctx := sql.NewContext(context.TODO(), sql.WithSession(session))

	user, err := fn.Eval(ctx, nil)
	require.NoError(t, err)
	assert.Equal(t, "root", user)

	session = sql.NewSession("server", "client", "someguy", 0)
	ctx = sql.NewContext(context.TODO(), sql.WithSession(session))

	user, err = fn.Eval(ctx, nil)
	require.NoError(t, err)
	assert.Equal(t, "someguy", user)

	ctx = sql.NewEmptyContext()

	user, err = fn.Eval(ctx, nil)
	require.NoError(t, err)
	assert.Equal(t, "", user)
}
