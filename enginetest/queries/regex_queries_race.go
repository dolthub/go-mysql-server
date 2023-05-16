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

//go:build race

// Running tests with `-race` will cause issues with our regex implementation. Memory usage skyrockets, and execution
// speed grinds to a halt as the pagefile/swap gets involved. Therefore, we do not run any regex tests when using the
// `-race` flag.

package queries

import (
	"gopkg.in/src-d/go-errors.v1"

	"github.com/dolthub/go-mysql-server/sql"
)

type RegexTest struct {
	Query       string
	Expected    []sql.Row
	ExpectedErr *errors.Kind
}

var RegexTests []RegexTest
