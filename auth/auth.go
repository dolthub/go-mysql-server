package auth

import (
	"strings"

	"gopkg.in/src-d/go-errors.v1"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
	"gopkg.in/src-d/go-vitess.v1/mysql"
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
