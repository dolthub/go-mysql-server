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
	"testing"

	"github.com/dolthub/go-mysql-server/auth"
)

func TestNoneAuthentication(t *testing.T) {
	a := new(auth.None)

	tests := []authenticationTest{
		{"root", "", true},
		{"root", "password", true},
		{"root", "mysql_password", true},
		{"user", "password", true},
		{"user", "other_password", true},
		{"user", "", true},
		{"", "", true},
		{"", "password", true},
	}

	testAuthentication(t, a, tests, nil)
}

func TestNoneAuthorization(t *testing.T) {
	a := new(auth.None)

	tests := []authorizationTest{
		{"user", queries["select"], true},
		{"root", queries["select"], true},
		{"", queries["select"], true},

		{"user", queries["create_index"], true},

		{"root", queries["drop_index"], true},

		{"", queries["insert"], true},

		{"user", queries["lock"], true},

		{"root", queries["unlock"], true},
	}

	testAuthorization(t, a, tests, nil)
}
