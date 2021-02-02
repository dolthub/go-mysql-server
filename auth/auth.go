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

package auth

import (
	"strings"

	"github.com/dolthub/vitess/go/mysql"
	"gopkg.in/src-d/go-errors.v1"

	"github.com/dolthub/go-mysql-server/sql"
)

// Permission holds permissions required by a query or grated to a user.
type Permission int

const (
	// ReadPerm means that it reads.
	ReadPerm Permission = 1 << iota
	// WritePerm means that it writes.
	WritePerm
)

var (
	// AllPermissions hold all defined permissions.
	AllPermissions = ReadPerm | WritePerm
	// DefaultPermissions are the permissions granted to a user if not defined.
	DefaultPermissions = ReadPerm

	// PermissionNames is used to translate from human to machine
	// representations.
	PermissionNames = map[string]Permission{
		"read":  ReadPerm,
		"write": WritePerm,
	}

	// ErrNotAuthorized is returned when the user is not allowed to use a
	// permission.
	ErrNotAuthorized = errors.NewKind("not authorized")
	// ErrNoPermission is returned when the user lacks needed permissions.
	ErrNoPermission = errors.NewKind("user does not have permission: %s")
)

// String returns all the permissions set to on.
func (p Permission) String() string {
	var str []string
	for k, v := range PermissionNames {
		if p&v != 0 {
			str = append(str, k)
		}
	}

	return strings.Join(str, ", ")
}

// Auth interface provides mysql authentication methods and permission checking
// for users.
type Auth interface {
	// Mysql returns a configured authentication method used by server.Server.
	Mysql() mysql.AuthServer
	// Allowed checks user's permissions with needed permission. If the user
	// does not have enough permissions it returns ErrNotAuthorized.
	// Otherwise is an error using the authentication method.
	Allowed(ctx *sql.Context, permission Permission) error
}
