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
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"regexp"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/vitess/go/mysql"
	"gopkg.in/src-d/go-errors.v1"
)

var (
	regNative = regexp.MustCompile(`^\*[0-9A-F]{40}$`)

	// ErrParseUserFile is given when user file is malformed.
	ErrParseUserFile = errors.NewKind("error parsing user file")
	// ErrUnknownPermission happens when a user permission is not defined.
	ErrUnknownPermission = errors.NewKind("unknown permission, %s")
	// ErrDuplicateUser happens when a user appears more than once.
	ErrDuplicateUser = errors.NewKind("duplicate user, %s")
)

// nativeUser holds information about credentials and permissions for a user.
type nativeUser struct {
	Name            string
	Password        string
	JSONPermissions []string `json:"Permissions"`
	Permissions     Permission
}

// Allowed checks if the user has certain permission.
func (u nativeUser) Allowed(p Permission) error {
	if u.Permissions&p == p {
		return nil
	}

	// permissions needed but not granted to the user
	p2 := (^u.Permissions) & p

	return ErrNotAuthorized.Wrap(ErrNoPermission.New(p2))
}

// NativePassword generates a mysql_native_password string.
func NativePassword(password string) string {
	if len(password) == 0 {
		return ""
	}

	// native = sha1(sha1(password))

	hash := sha1.New()
	hash.Write([]byte(password))
	s1 := hash.Sum(nil)

	hash.Reset()
	hash.Write(s1)
	s2 := hash.Sum(nil)

	s := strings.ToUpper(hex.EncodeToString(s2))

	return fmt.Sprintf("*%s", s)
}

// Native holds mysql_native_password users.
type Native struct {
	users map[string]nativeUser
}

// NewNativeSingle creates a NativeAuth with a single user with given
// permissions.
func NewNativeSingle(name, password string, perm Permission) *Native {
	users := make(map[string]nativeUser)
	users[name] = nativeUser{
		Name:        name,
		Password:    NativePassword(password),
		Permissions: perm,
	}

	return &Native{users}
}

// NewNativeFile creates a NativeAuth and loads users from a JSON file.
func NewNativeFile(file string) (*Native, error) {
	var data []nativeUser

	raw, err := ioutil.ReadFile(file)
	if err != nil {
		return nil, ErrParseUserFile.New(err)
	}

	if err := json.Unmarshal(raw, &data); err != nil {
		return nil, ErrParseUserFile.New(err)
	}

	users := make(map[string]nativeUser)
	for _, u := range data {
		_, ok := users[u.Name]
		if ok {
			return nil, ErrParseUserFile.Wrap(ErrDuplicateUser.New(u.Name))
		}

		if !regNative.MatchString(u.Password) {
			u.Password = NativePassword(u.Password)
		}

		if len(u.JSONPermissions) == 0 {
			u.Permissions = DefaultPermissions
		}

		for _, p := range u.JSONPermissions {
			perm, ok := PermissionNames[strings.ToLower(p)]
			if !ok {
				return nil, ErrParseUserFile.Wrap(ErrUnknownPermission.New(p))
			}

			u.Permissions |= perm
		}

		users[u.Name] = u
	}

	return &Native{users}, nil
}

// Mysql implements Auth interface.
func (s *Native) Mysql() mysql.AuthServer {
	auth := mysql.NewAuthServerStatic()

	for k, v := range s.users {
		auth.Entries[k] = []*mysql.AuthServerStaticEntry{
			{
				MysqlNativePassword: v.Password,
				Password:            v.Password},
		}
	}

	return auth
}

// Allowed implements Auth interface.
func (s *Native) Allowed(ctx *sql.Context, permission Permission) error {
	name := ctx.Client().User
	u, ok := s.users[name]
	if !ok {
		return ErrNotAuthorized.Wrap(ErrNoPermission.New(permission))
	}

	return u.Allowed(permission)
}
