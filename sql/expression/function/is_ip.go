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
	"fmt"
	"github.com/dolthub/go-mysql-server/sql"
	"math/big"
	"net"
	"reflect"
	"strings"
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
	return &IsIP{val, false, true, false}
}

func NewIsIP4Mapped(val sql.Expression) sql.Expression {
	return &IsIP{val, false, false, true}
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
	// TODO: Maybe they shouldn't all be together; this kind of messy and confusing

	// Evaluate value
	val, err := i.val.Eval(ctx, row)
	if err != nil {
		return nil, err
	}

	// Expect to receive IPv4 Address, otherwise receiving expect ipv6 (big) int
	if i.compat || i.mapped {
		// Convert to into string, then into big int
		val, err = sql.LongText.Convert(val)
		if err != nil {
			return nil, sql.ErrInvalidType.New(reflect.TypeOf(val).String())
		}
		ipv6int := new(big.Int)
		ipv6int, ok := ipv6int.SetString(val.(string), 10)
		if !ok {
			// TODO: figure out right error
			return nil, sql.ErrInvalidType.New(reflect.TypeOf(val).String())
		}

		// Check if IPv4 compatible
		if i.compat {
			compatPrefix := new(big.Int)
			compatPrefix.SetString("0", 10)
			ipv6int.Rsh(ipv6int, 32)
			return ipv6int.Cmp(compatPrefix) == 0, nil
		}

		// Check if IPv4 mapped
		mappedPrefix := new(big.Int)
		mappedPrefix.SetString("FFFF", 16)
		ipv6int.Rsh(ipv6int, 32)
		return ipv6int.Cmp(mappedPrefix) == 0, nil
	}

	// Parse IP address, return false if not valid ip
	ip := net.ParseIP(val.(string))
	if ip == nil {
		return false, nil
	}

	// Check if ip address is valid IPv4 address
	if i.ipv4 {
		return ip.To4() != nil, nil
	}

	return ip.To16() != nil && (strings.Count(val.(string),":") >= 2), nil
}

// IsNullable implements the Expression interface
func (i IsIP) IsNullable() bool {
	return i.val.IsNullable()
}

func (i IsIP) String() string {
	if i.compat {
		return fmt.Sprintf("IS_IP4_COMPAT(%s)", i.val)
	} else if i.mapped {
		return fmt.Sprintf("IS_IP4_MAPPED(%s)", i.val)
	} else if i.ipv4 {
		return fmt.Sprintf("IS_IP4(%s)", i.val)
	} else {
		return fmt.Sprintf("IS_IP6(%s)", i.val)
	}
}

func (i IsIP) Resolved() bool {
	return i.val.Resolved()
}

func (IsIP) Type() sql.Type { return sql.LongText }

func (i IsIP) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(i, len(children), 1)
	}
	if i.compat {
		return NewIsIP4Compat(i.val), nil
	} else if i.mapped {
		return NewIsIP4Mapped(i.val), nil
	} else if i.ipv4 {
		return NewIsIP4(i.val), nil
	} else {
		return NewIsIP6(i.val), nil
	}
}