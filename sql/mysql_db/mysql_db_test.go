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
	db, p := newMySQLDbWithPersistence()

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

// account is a user to create in a test database.
type account struct {
	user, host, password string
}

// newMySQLDbWithPersistence returns an empty MySQLDb whose Persist payloads
// are captured by the returned persistence.
func newMySQLDbWithPersistence() (*MySQLDb, *capturingPersistence) {
	db := CreateEmptyMySQLDb()
	p := &capturingPersistence{}
	db.SetPersister(p)
	return db, p
}

// withEditor runs cb against an Editor for db, closing it afterwards.
func withEditor(db *MySQLDb, cb func(ed *Editor)) {
	ed := db.Editor()
	defer ed.Close()
	cb(ed)
}

// primaryPayload returns the serialized users and grants of a database
// containing the given accounts. This is the payload a primary replicates to
// its replicas and which they apply with OverwriteUsersAndGrantData.
func primaryPayload(t *testing.T, ctx *sql.Context, accounts ...account) []byte {
	primary, persistence := newMySQLDbWithPersistence()
	withEditor(primary, func(ed *Editor) {
		for _, a := range accounts {
			primary.AddSuperUser(ed, a.user, a.host, a.password)
		}
		require.NoError(t, primary.Persist(ctx, ed))
	})
	require.NotNil(t, persistence.buf)
	return persistence.buf
}

// newReplica returns a database standing in for a replica, with an ephemeral
// super user of its own creation, of the kind a server instance adds for local
// CLI access.
func newReplica(ephemeral account) (*MySQLDb, *capturingPersistence) {
	replica, persistence := newMySQLDbWithPersistence()
	withEditor(replica, func(ed *Editor) {
		replica.AddEphemeralSuperUser(ed, ephemeral.user, ephemeral.host, ephemeral.password)
	})
	return replica, persistence
}

// TestMySQLDbOverwriteUsersAndGrantsDataEphemeralUsers asserts that an
// overwrite, which is how a replica applies a primary's users and grants, keeps
// the ephemeral users of the process it is applied to. They are never part of a
// payload, since Persist skips them, so dropping them would leave a replica
// without the accounts its own server instance created.
func TestMySQLDbOverwriteUsersAndGrantsDataEphemeralUsers(t *testing.T) {
	ctx := sql.NewEmptyContext()

	t.Run("KeptAcrossOverwrite", func(t *testing.T) {
		payload := primaryPayload(t, ctx,
			account{"root", "localhost", ""},
			account{"aaron", "%", "aaronspassword"})

		replica, replicaPersistence := newReplica(account{"ephemeral_user", "localhost", "ephemeralpassword"})
		withEditor(replica, func(ed *Editor) {
			require.NoError(t, replica.OverwriteUsersAndGrantData(ctx, ed, payload))
			require.NoError(t, replica.Persist(ctx, ed))
		})

		rd := replica.Reader()
		defer rd.Close()

		require.NotNil(t, replica.GetUser(rd, "aaron", "%", false))

		ephemeral := replica.GetUser(rd, "ephemeral_user", "localhost", false)
		require.NotNil(t, ephemeral)
		require.True(t, ephemeral.IsEphemeral)
		require.True(t, ephemeral.IsSuperUser)

		// The ephemeral user is still not persisted, so the replica's persisted
		// copy is only what the primary sent.
		replicaOfPayload := CreateEmptyMySQLDb()
		require.NoError(t, replicaOfPayload.LoadData(ctx, replicaPersistence.buf))
		rd2 := replicaOfPayload.Reader()
		defer rd2.Close()
		require.NotNil(t, replicaOfPayload.GetUser(rd2, "aaron", "%", false))
		require.Nil(t, replicaOfPayload.GetUser(rd2, "ephemeral_user", "localhost", false))
	})

	t.Run("TakePrecedenceOverPayload", func(t *testing.T) {
		payload := primaryPayload(t, ctx, account{"shared_name", "localhost", "primarypassword"})

		replica, _ := newReplica(account{"shared_name", "localhost", "ephemeralpassword"})

		rd := replica.Reader()
		ephemeralAuthString := replica.GetUser(rd, "shared_name", "localhost", false).AuthString
		rd.Close()

		withEditor(replica, func(ed *Editor) {
			require.NoError(t, replica.OverwriteUsersAndGrantData(ctx, ed, payload))
		})

		rd = replica.Reader()
		defer rd.Close()
		user := replica.GetUser(rd, "shared_name", "localhost", false)
		require.NotNil(t, user)
		require.True(t, user.IsEphemeral)
		require.Equal(t, ephemeralAuthString, user.AuthString)
	})
}

func TestMatchesHostPattern(t *testing.T) {
	tests := []struct {
		name     string
		host     string
		pattern  string
		expected bool
	}{
		// Basic wildcard patterns
		{"IP wildcard - exact match", "127.0.0.1", "127.0.0.%", true},
		{"IP wildcard - different last octet", "127.0.0.255", "127.0.0.%", true},
		{"IP wildcard - no match", "192.168.1.1", "127.0.0.%", false},
		{"IP wildcard - partial match", "127.0.1.1", "127.0.0.%", false},

		// Multiple wildcards
		{"Multiple wildcards", "192.168.1.100", "192.168.%.%", true},
		{"Multiple wildcards - no match", "10.0.1.100", "192.168.%.%", false},

		// Single wildcard at different positions
		{"Wildcard first octet", "10.0.0.1", "%.0.0.1", true},
		{"Wildcard middle octet", "192.168.50.1", "192.%.50.1", true},
		{"Wildcard last octet", "192.168.1.255", "192.168.1.%", true},

		// Non-IP patterns
		{"Hostname wildcard", "server1.example.com", "server%.example.com", true},
		{"Hostname wildcard - no match", "db1.example.com", "server%.example.com", false},
		{"Domain wildcard", "host.subdomain.example.com", "%.example.com", true},

		// Edge cases
		{"Empty pattern", "127.0.0.1", "", false},
		{"Pattern without wildcard", "127.0.0.1", "127.0.0.1", false}, // Should return false as it's not a wildcard pattern
		{"Just wildcard", "anything", "%", true},
		{"Multiple wildcards together", "test", "%%", true},

		// Special characters in patterns (should be escaped)
		{"Pattern with dots", "test.host", "test.%", true},
		{"Pattern with regex chars", "test[1]", "test[%]", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := matchesHostPattern(tt.host, tt.pattern)
			require.Equal(t, tt.expected, result, "matchesHostPattern(%q, %q) = %v, want %v", tt.host, tt.pattern, result, tt.expected)
		})
	}
}

func TestGetUserWithWildcardAuthentication(t *testing.T) {
	ctx := sql.NewEmptyContext()
	db, _ := newMySQLDbWithPersistence()

	// Add test users with various host patterns
	ed := db.Editor()
	db.AddSuperUser(ed, "testuser", "127.0.0.1", "password")
	db.AddSuperUser(ed, "localhost_user", "localhost", "password")
	db.AddSuperUser(ed, "wildcard_user", "127.0.0.%", "password")
	db.AddSuperUser(ed, "subnet_user", "192.168.1.%", "password")
	db.AddSuperUser(ed, "hostname_user", "%.example.com", "password")
	db.AddSuperUser(ed, "any_user", "%", "password")
	db.Persist(ctx, ed)
	ed.Close()

	rd := db.Reader()
	defer rd.Close()

	tests := []struct {
		name         string
		username     string
		host         string
		expectedUser string
		shouldFind   bool
	}{
		// Specific IP tests
		{"Specific IP - exact match", "testuser", "127.0.0.1", "testuser", true},
		{"Localhost user - normalized", "localhost_user", "127.0.0.1", "localhost_user", true},
		{"Localhost user - ::1", "localhost_user", "::1", "localhost_user", true},
		{"Non-existent user", "nonexistent", "127.0.0.1", "", false},

		// IP wildcard tests
		{"Wildcard IP - 127.0.0.% matches 127.0.0.1", "wildcard_user", "127.0.0.1", "wildcard_user", true},
		{"Wildcard IP - 127.0.0.% matches 127.0.0.100", "wildcard_user", "127.0.0.100", "wildcard_user", true},
		{"Wildcard IP - 127.0.0.% matches 127.0.0.255", "wildcard_user", "127.0.0.255", "wildcard_user", true},
		{"Wildcard IP - 127.0.0.% does not match 127.0.1.1", "wildcard_user", "127.0.1.1", "", false},
		{"Wildcard IP - 127.0.0.% does not match 192.168.1.1", "wildcard_user", "192.168.1.1", "", false},

		// Subnet wildcard tests
		{"Subnet wildcard - 192.168.1.% matches 192.168.1.1", "subnet_user", "192.168.1.1", "subnet_user", true},
		{"Subnet wildcard - 192.168.1.% matches 192.168.1.100", "subnet_user", "192.168.1.100", "subnet_user", true},
		{"Subnet wildcard - 192.168.1.% does not match 192.168.2.1", "subnet_user", "192.168.2.1", "", false},
		{"Subnet wildcard - 192.168.1.% does not match 10.0.0.1", "subnet_user", "10.0.0.1", "", false},

		// Hostname wildcard tests
		{"Hostname wildcard - %.example.com matches host.example.com", "hostname_user", "host.example.com", "hostname_user", true},
		{"Hostname wildcard - %.example.com matches www.example.com", "hostname_user", "www.example.com", "hostname_user", true},
		{"Hostname wildcard - %.example.com does not match example.com", "hostname_user", "example.com", "", false},
		{"Hostname wildcard - %.example.com does not match host.other.com", "hostname_user", "host.other.com", "", false},

		// Global wildcard tests
		{"Global wildcard - % matches any IP", "any_user", "10.0.0.1", "any_user", true},
		{"Global wildcard - % matches any hostname", "any_user", "any.hostname.com", "any_user", true},
		{"Global wildcard - % matches localhost", "any_user", "localhost", "any_user", true},

		// Issue #9624 scenario
		{"Customer scenario - matches connecting IP in range", "wildcard_user", "127.0.0.50", "wildcard_user", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			user := db.GetUser(rd, tt.username, tt.host, false)

			if !tt.shouldFind {
				require.Nil(t, user, "Expected no user to be found for %s@%s", tt.username, tt.host)
				return
			}

			require.NotNil(t, user, "Expected user to be found for %s@%s", tt.username, tt.host)
			require.Equal(t, tt.expectedUser, user.User, "Expected username %s, got %s", tt.expectedUser, user.User)
		})
	}
}
