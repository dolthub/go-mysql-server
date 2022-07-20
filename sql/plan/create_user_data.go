// Copyright 2021 Dolthub, Inc.
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

package plan

import (
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"strings"
)

// UserName represents either a user or role name.
type UserName struct {
	Name    string
	Host    string
	AnyHost bool
}

// String returns the UserName as a formatted string using the quotes given. Using the default root
// account with the backtick as the quote, root@localhost would become `root`@`localhost`. Different quotes are used
// in different places in MySQL. In addition, if the quote is used in a section as part of the name, it is escaped by
// doubling the quote (which also mimics MySQL behavior).
func (un *UserName) String(quote string) string {
	host := un.Host
	if un.AnyHost {
		host = "%"
	}
	replacement := quote + quote
	name := strings.ReplaceAll(un.Name, quote, replacement)
	host = strings.ReplaceAll(host, quote, replacement)
	return fmt.Sprintf("%s%s%s@%s%s%s", quote, name, quote, quote, host, quote)
}

// Authentication represents an authentication method for a user.
type Authentication interface {
	// Plugin returns the name of the plugin that this authentication represents.
	Plugin() string
	// Password returns the value to insert into the database as the password.
	Password() string
}

// AuthenticatedUser represents a user with the relevant methods of authentication.
type AuthenticatedUser struct {
	UserName
	Auth1       Authentication
	Auth2       Authentication
	Auth3       Authentication
	AuthInitial Authentication
	Identity    string
}

// TLSOptions represents a user's TLS options.
type TLSOptions struct {
	SSL     bool
	X509    bool
	Cipher  string
	Issuer  string
	Subject string
}

// AccountLimits represents the limits imposed upon an account.
type AccountLimits struct {
	MaxQueriesPerHour     *int64
	MaxUpdatesPerHour     *int64
	MaxConnectionsPerHour *int64
	MaxUserConnections    *int64
}

// PasswordOptions states how to handle a user's passwords.
type PasswordOptions struct {
	RequireCurrentOptional bool

	ExpirationTime *int64
	History        *int64
	ReuseInterval  *int64
	FailedAttempts *int64
	LockTime       *int64
}

// AuthenticationMysqlNativePassword is an authentication type that represents "mysql_native_password".
type AuthenticationMysqlNativePassword string

var _ Authentication = AuthenticationMysqlNativePassword("")

// Plugin implements the interface Authentication.
func (a AuthenticationMysqlNativePassword) Plugin() string {
	return "mysql_native_password"
}

// Password implements the interface Authentication.
func (a AuthenticationMysqlNativePassword) Password() string {
	if len(a) == 0 {
		return ""
	}
	// native = sha1(sha1(password))
	hash := sha1.New()
	hash.Write([]byte(a))
	s1 := hash.Sum(nil)
	hash.Reset()
	hash.Write(s1)
	s2 := hash.Sum(nil)
	return "*" + strings.ToUpper(hex.EncodeToString(s2))
}

// NewDefaultAuthentication returns the given password with the default
// authentication method.
func NewDefaultAuthentication(password string) Authentication {
	return AuthenticationMysqlNativePassword(password)
}

// AuthenticationOther is an authentication type that represents plugin types
// other than "mysql_native_password". There must be a mysqldb plugin provided
// to use this plugin.
type AuthenticationOther struct {
	password string
	plugin   string
}

func NewOtherAuthentication(password, plugin string) Authentication {
	return AuthenticationOther{password, plugin}
}

func (a AuthenticationOther) Plugin() string {
	return a.plugin
}

func (a AuthenticationOther) Password() string {
	return string(a.password)
}
