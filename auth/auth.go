package auth

import (
	"strings"

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

	// PermissionNames is used to translate from human and machine
	// representations.
	PermissionNames = map[string]Permission{
		"read":  ReadPerm,
		"write": WritePerm,
	}

	// ErrNoPermission is returned when the user lacks needed permissions.
	ErrNoPermission = "user does not have permission: %s"
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
	Mysql() mysql.AuthServer
	Allowed(user string, permission Permission) error
}
