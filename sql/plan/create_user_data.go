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
	"fmt"
	"strings"
)

// UserName represents either a user or role name.
type UserName struct {
	Name    string
	Host    string
	AnyHost bool
}

// String returns the UserName as a formatted string, using "`" to quote each part.
func (un *UserName) String() string {
	return un.StringWithQuote("`", "``")
}

// StringWithQuote returns the UserName as a formatted string using the quotes given. If a replacement is given, then
// also replaces any existing instances of the quotes with the replacement.
func (un *UserName) StringWithQuote(quote string, replacement string) string {
	name := un.Name
	host := un.Host
	if un.AnyHost {
		host = "%"
	}
	if len(replacement) > 0 {
		name = strings.ReplaceAll(name, quote, replacement)
		host = strings.ReplaceAll(host, quote, replacement)
	}
	return fmt.Sprintf("%s%s%s@%s%s%s", quote, name, quote, quote, host, quote)
}

// Authentication represents an authentication method for a user.
type Authentication interface{} //TODO: add these

// AuthenticatedUser represents a user with the relevant methods of authentication.
type AuthenticatedUser struct {
	UserName
	Auth1       Authentication
	Auth2       Authentication
	Auth3       Authentication
	AuthInitial Authentication
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
