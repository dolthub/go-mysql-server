// Copyright 2020-2021 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package auth_test

import (
	"context"
	"testing"
	"time"

	"github.com/dolthub/go-mysql-server/auth"
	"github.com/dolthub/go-mysql-server/sql"

	"github.com/sanity-io/litter"
	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
)

type Authentication struct {
	user    string
	address string
	err     error
}

type Authorization struct {
	ctx *sql.Context
	p   auth.Permission
	err error
}

type Query struct {
	ctx *sql.Context
	d   time.Duration
	err error
}

type auditTest struct {
	authentication Authentication
	authorization  Authorization
	query          Query
}

func (a *auditTest) Authentication(user string, address string, err error) {
	a.authentication = Authentication{
		user:    user,
		address: address,
		err:     err,
	}
}

func (a *auditTest) Authorization(ctx *sql.Context, p auth.Permission, err error) {
	a.authorization = Authorization{
		ctx: ctx,
		p:   p,
		err: err,
	}
}

func (a *auditTest) Query(ctx *sql.Context, d time.Duration, err error) {
	println("query!")
	a.query = Query{
		ctx: ctx,
		d:   d,
		err: err,
	}
}

func (a *auditTest) Clean() {
	a.authorization = Authorization{}
	a.authentication = Authentication{}
	a.query = Query{}
}

func TestAuditAuthentication(t *testing.T) {
	a := auth.NewNativeSingle("user", "password", auth.AllPermissions)
	at := new(auditTest)
	audit := auth.NewAudit(a, at)

	extra := func(t *testing.T, c authenticationTest) {
		a := at.authentication

		require.Equal(t, c.user, a.user)
		require.NotEmpty(t, a.address)
		if c.success {
			require.NoError(t, a.err)
		} else {
			require.Error(t, a.err)
			require.Nil(t, at.authorization.ctx)
			require.Nil(t, at.query.ctx)
		}

		at.Clean()
	}

	testAuthentication(t, audit, nativeSingleTests, extra)
}

func TestAuditAuthorization(t *testing.T) {
	a := auth.NewNativeSingle("user", "", auth.ReadPerm)
	at := new(auditTest)
	audit := auth.NewAudit(a, at)

	tests := []authorizationTest{
		{"user", "invalid query", false},
		{"user", queries["select"], true},
		{"user", queries["create_index"], false},
		{"user", queries["drop_index"], false},
		{"user", queries["insert"], false},
		{"user", queries["lock"], false},
		{"user", queries["unlock"], false},
	}

	extra := func(t *testing.T, c authorizationTest) {
		a := at.authorization
		q := at.query

		litter.Dump(q)
		require.NotNil(t, q.ctx)
		require.Equal(t, c.user, q.ctx.Client().User)
		require.NotEmpty(t, q.ctx.Client().Address)
		// TODO: this fails, at least on my Windows PC, because the time resolution isn't fine grained enough (duration is
		//  zero). We either need a fake clock or a better way of measuring that something happened.
		// require.NotZero(t, q.d)
		require.Equal(t, c.user, at.authentication.user)

		if c.success {
			require.Equal(t, c.user, a.ctx.Client().User)
			require.NotEmpty(t, a.ctx.Client().Address)
			require.NoError(t, a.err)
			require.NoError(t, q.err)
		} else {
			require.Error(t, q.err)

			// if there's a syntax error authorization is not triggered
			if auth.ErrNotAuthorized.Is(q.err) {
				require.Equal(t, q.err, a.err)
				require.NotNil(t, a.ctx)
				require.Equal(t, c.user, a.ctx.Client().User)
				require.NotEmpty(t, a.ctx.Client().Address)
			} else {
				require.NoError(t, a.err)
				require.Nil(t, a.ctx)
			}
		}

		at.Clean()
	}

	testAudit(t, audit, tests, extra)
}

func TestAuditLog(t *testing.T) {
	require := require.New(t)

	logger, hook := test.NewNullLogger()
	l := auth.NewAuditLog(logger)

	pid := uint64(303)
	id := uint32(42)

	l.Authentication("user", "client", nil)
	e := hook.LastEntry()
	require.NotNil(e)
	require.Equal(logrus.InfoLevel, e.Level)
	m := logrus.Fields{
		"system":  "audit",
		"action":  "authentication",
		"user":    "user",
		"address": "client",
		"success": true,
	}
	require.Equal(m, e.Data)

	err := auth.ErrNoPermission.New(auth.ReadPerm)
	l.Authentication("user", "client", err)
	e = hook.LastEntry()
	m["success"] = false
	m["err"] = err
	require.Equal(m, e.Data)

	s := sql.NewSession("server", sql.Client{Address: "client", User: "user"}, id)
	ctx := sql.NewContext(context.TODO(),
		sql.WithSession(s),
		sql.WithPid(pid),
		sql.WithQuery("query"),
	)

	l.Authorization(ctx, auth.ReadPerm, nil)
	e = hook.LastEntry()
	require.NotNil(e)
	require.Equal(logrus.InfoLevel, e.Level)
	m = logrus.Fields{
		"system":        "audit",
		"action":        "authorization",
		"permission":    auth.ReadPerm.String(),
		"user":          "user",
		"query":         "query",
		"address":       "client",
		"connection_id": id,
		"pid":           pid,
		"success":       true,
	}
	require.Equal(m, e.Data)

	l.Authorization(ctx, auth.ReadPerm, err)
	e = hook.LastEntry()
	m["success"] = false
	m["err"] = err
	require.Equal(m, e.Data)

	l.Query(ctx, 808*time.Second, nil)
	e = hook.LastEntry()
	require.NotNil(e)
	require.Equal(logrus.InfoLevel, e.Level)
	m = logrus.Fields{
		"system":        "audit",
		"action":        "query",
		"duration":      808 * time.Second,
		"user":          "user",
		"query":         "query",
		"address":       "client",
		"connection_id": id,
		"pid":           pid,
		"success":       true,
	}
	require.Equal(m, e.Data)

	l.Query(ctx, 808*time.Second, err)
	e = hook.LastEntry()
	m["success"] = false
	m["err"] = err
	require.Equal(m, e.Data)
}
