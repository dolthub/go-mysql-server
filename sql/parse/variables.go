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

package parse

import (
	"bufio"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
)

func parseShowVariables(ctx *sql.Context, s string) (sql.Node, error) {
	var pattern string

	r := bufio.NewReader(strings.NewReader(s))
	for _, fn := range []parseFunc{
		expect("show"),
		skipSpaces,
		func(in *bufio.Reader) error {
			var s string
			if err := readIdent(&s)(in); err != nil {
				return err
			}

			switch s {
			case "global", "session":
				if err := skipSpaces(in); err != nil {
					return err
				}

				return expect("variables")(in)
			case "variables":
				return nil
			}
			return errUnexpectedSyntax.New("show [global | session] variables", s)
		},
		skipSpaces,
		func(in *bufio.Reader) error {
			if expect("like")(in) == nil {
				if err := skipSpaces(in); err != nil {
					return err
				}

				if err := readValue(&pattern)(in); err != nil {
					return err
				}
			}
			return nil
		},
		skipSpaces,
		checkEOF,
	} {
		if err := fn(r); err != nil {
			return nil, err
		}
	}

	return plan.NewShowVariables(pattern), nil
}
