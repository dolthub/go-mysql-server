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
	db := CreateEmptyMySQLDb()
	p := &capturingPersistence{}
	db.SetPersister(p)

	// Add test users with various host patterns
	ed := db.Editor()
	db.AddSuperUser(ed, "testuser", "127.0.0.1", "password")
	db.AddSuperUser(ed, "localhost_user", "localhost", "password")
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
		// Test specific IP matching (existing functionality)
		{"Specific IP - exact match", "testuser", "127.0.0.1", "testuser", true},
		{"Localhost user - normalized", "localhost_user", "127.0.0.1", "localhost_user", true},
		{"Localhost user - ::1", "localhost_user", "::1", "localhost_user", true},
		{"Non-existent user", "nonexistent", "127.0.0.1", "", false},
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
