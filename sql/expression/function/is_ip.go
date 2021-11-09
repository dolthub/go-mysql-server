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

package function

import (

	"github.com/dolthub/go-mysql-server/sql"
	"golang.org/x/net/ipv4"
)

type IsIP struct {
	val sql.Expression
	ipv4, mapped, compat bool
}

var _ sql.FunctionExpression = (*IsIP)(nil)

func NewIsIP6(val sql.Expression) sql.Expression {
	return &IsIP{val, false, false, false}
}

func NewIsIP4(val sql.Expression) sql.Expression {
	return &IsIP{val, true, false, false}
}

func NewIsIP4Compat(val sql.Expression) sql.Expression {
	return &IsIP{val, true, true, false}
}

func NewIsIP4Mapped(val sql.Expression) sql.Expression {
	return &IsIP{val, true, false, true}
}

func (i *IsIP) FunctionName() string {
	if i.compat {
		return "is_ip4_compat"
	} else if i.mapped{
		return "is_ip4_mapped"
	} else if i.ipv4{
		return "is_ip4"
	} else {
		return "is_ip6"
	}
}

// Children implements the Expression interface
func (i *IsIP) Children() []sql.Expression {
	return []sql.Expression{i.val}
}

// Eval implements the Expression interface
func (i *IsIP) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	// Evaluate value
	val, err := i.val.Eval(ctx, row)
	if err != nil {
		return nil, err
	}

	// Check if ip address is valid IPv4-compatible IPv6 address
	if i.compat {

		return false, nil
	}

	// Check if ip address is valid IPv4 address
	if i.ipv4 {

		return false, nil
	}

	// Check based on function call
	if i.compat {
		return false, nil
	}

	// Check based on function call
	if i.compat {
		return false, nil
	}
}