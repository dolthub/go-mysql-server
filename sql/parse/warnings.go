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
	"bytes"
	"io"
	"strconv"
	"strings"
	"unicode"

	errors "gopkg.in/src-d/go-errors.v1"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/plan"
)

var errInvalidIndex = errors.NewKind("invalid %s index %d (index must be non-negative)")

func parseShowWarnings(ctx *sql.Context, s string) (sql.Node, error) {
	var (
		offstr string
		cntstr string
	)

	r := bufio.NewReader(strings.NewReader(s))
	for _, fn := range []parseFunc{
		expect("show"),
		skipSpaces,
		expect("warnings"),
		skipSpaces,
		func(in *bufio.Reader) error {
			if expect("limit")(in) == nil {
				skipSpaces(in)
				readValue(&cntstr)(in)
				skipSpaces(in)
				if expectRune(',')(in) == nil {
					if readValue(&offstr)(in) == nil {
						offstr, cntstr = cntstr, offstr
					}
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

	var (
		node   sql.Node = plan.ShowWarnings(ctx.Session.Warnings())
		offset int
		count  int
		err    error
	)
	if offstr != "" {
		if offset, err = strconv.Atoi(offstr); err != nil {
			return nil, err
		}
		if offset < 0 {
			return nil, errInvalidIndex.New("offset", offset)
		}
	}
	node = plan.NewOffset(expression.NewLiteral(offset, sql.Int64), node)
	if cntstr != "" {
		if count, err = strconv.Atoi(cntstr); err != nil {
			return nil, err
		}
		if count < 0 {
			return nil, errInvalidIndex.New("count", count)
		}
		if count > 0 {
			node = plan.NewLimit(expression.NewLiteral(count, sql.Int64), node)
		}
	}

	return node, nil
}

func readValue(val *string) parseFunc {
	return func(rd *bufio.Reader) error {
		var buf bytes.Buffer
		var singleQuote, doubleQuote, ignoreNext bool
		var first = true
		for {
			r, _, err := rd.ReadRune()
			if err == io.EOF {
				break
			}

			if err != nil {
				return err
			}

			if singleQuote || doubleQuote {
				switch true {
				case ignoreNext:
					ignoreNext = false
				case r == '\\':
					ignoreNext = true
					continue
				case r == '\'' && singleQuote:
					singleQuote = false
					continue
				case r == '"' && doubleQuote:
					doubleQuote = false
					continue
				}
			} else if first && (r == '\'' || r == '"') {
				if r == '\'' {
					singleQuote = true
				} else {
					doubleQuote = true
				}
				first = false
				continue
			} else if !unicode.IsLetter(r) && !unicode.IsNumber(r) && r != '_' {
				if err := rd.UnreadRune(); err != nil {
					return err
				}
				break
			}

			buf.WriteRune(r)
		}

		*val = strings.ToLower(buf.String())
		return nil
	}
}
