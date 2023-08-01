// Copyright 2023 Dolthub, Inc.
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

package mysql_db

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
)

type capturingPersistence struct {
	buf []byte
}

func (p *capturingPersistence) Persist(ctx *sql.Context, data []byte) error {
	p.buf = data
	return nil
}

func TestMySQLDbOverwriteUsersAndGrantsData(t *testing.T) {
	ctx := sql.NewEmptyContext()
	db := CreateEmptyMySQLDb()
	p := &capturingPersistence{}
	db.SetPersister(p)

	db.AddRootAccount()
	ed := db.Editor()
	db.Persist(ctx, ed)
	ed.Close()

	require.NotNil(t, p.buf)

	// A root@localhost user was created.
	rd := db.Reader()
	root := db.GetUser(rd, "root", "localhost", false)
	rd.Close()
	require.NotNil(t, root)

	onlyRoot := p.buf

	ed = db.Editor()
	db.AddSuperUser(ed, "aaron", "localhost", "")
	db.AddSuperUser(ed, "brian", "localhost", "")
	db.AddSuperUser(ed, "tim", "localhost", "")
	db.Persist(ctx, ed)
	ed.Close()

	var numUsers int
	rd = db.Reader()
	rd.VisitUsers(func(*User) {
		numUsers += 1
	})
	rd.Close()
	require.Equal(t, 4, numUsers)

	ed = db.Editor()
	require.NoError(t, db.OverwriteUsersAndGrantData(ctx, ed, onlyRoot))
	ed.Close()

	numUsers = 0
	rd = db.Reader()
	rd.VisitUsers(func(*User) {
		numUsers += 1
	})
	require.Equal(t, 1, numUsers)

	root = db.GetUser(rd, "root", "localhost", false)
	require.NotNil(t, root)

	tim := db.GetUser(rd, "tim", "localhost", false)
	require.Nil(t, tim)

	rd.Close()
}
