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
	"net"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
)

type IsIPv4 struct {
	val sql.Expression
}

var _ sql.FunctionExpression = (*IsIPv4)(nil)

func NewIsIPv4(val sql.Expression) sql.Expression {
	return &IsIPv4{val}
}

func (i *IsIPv4) FunctionName() string {
	return "is_ipv4"
}

// Children implements the Expression interface
func (i *IsIPv4) Children() []sql.Expression {
	return []sql.Expression{i.val}
}

// IsNullable implements the Expression interface
func (i IsIPv4) IsNullable() bool {
	return i.val.IsNullable()
}

func (i IsIPv4) String() string {
	return fmt.Sprintf("IS_IPV4(%s)", i.val)
}

func (i IsIPv4) Resolved() bool {
	return i.val.Resolved()
}

func (IsIPv4) Type() sql.Type { return sql.LongText }

func (i IsIPv4) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(i, len(children), 1)
	}
	return NewIsIPv4(i.val), nil
}

// Eval implements the Expression interface
func (i *IsIPv4) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	// Evaluate value
	val, err := i.val.Eval(ctx, row)
	if err != nil {
		return nil, err
	}

	// If null, return nul
	if val == nil {
		return nil, nil
	}

	// Must be of type string
	switch val.(type) {
	case string:
		// Parse IP address, return false if not valid ip
		ip := net.ParseIP(val.(string))
		if ip == nil {
			return false, nil
		}

		// Check if ip address is valid IPv4 address
		return ip.To4() != nil, nil
	default:
		return false, nil
	}
}

type IsIPv6 struct {
	val sql.Expression
}

var _ sql.FunctionExpression = (*IsIPv6)(nil)

func NewIsIPv6(val sql.Expression) sql.Expression {
	return &IsIPv6{val}
}

func (i *IsIPv6) FunctionName() string {
	return "is_ipv6"
}

// Children implements the Expression interface
func (i *IsIPv6) Children() []sql.Expression {
	return []sql.Expression{i.val}
}

// IsNullable implements the Expression interface
func (i IsIPv6) IsNullable() bool {
	return i.val.IsNullable()
}

func (i IsIPv6) String() string {
	return fmt.Sprintf("IS_IPV6(%s)", i.val)
}

func (i IsIPv6) Resolved() bool {
	return i.val.Resolved()
}

func (IsIPv6) Type() sql.Type { return sql.LongText }

func (i IsIPv6) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(i, len(children), 1)
	}
	return NewIsIPv6(i.val), nil
}

// Eval implements the Expression interface
func (i *IsIPv6) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	// Evaluate value
	val, err := i.val.Eval(ctx, row)
	if err != nil {
		return nil, err
	}

	// If null, return nul
	if val == nil {
		return nil, nil
	}

	// Must be of type string
	switch val.(type) {
	case string:
		// Parse IP address, return false if not valid ip
		ip := net.ParseIP(val.(string))
		if ip == nil {
			return false, nil
		}

		// Check if ip address is valid IPv6 address
		return ip.To16() != nil && (strings.Count(val.(string), ":") >= 2), nil
	default:
		return false, nil
	}
}



type IsIPv4Compat struct {
	val sql.Expression
}

var _ sql.FunctionExpression = (*IsIPv4Compat)(nil)

func NewIsIPv4Compat(val sql.Expression) sql.Expression {
	return &IsIPv4Compat{val}
}

func (i *IsIPv4Compat) FunctionName() string {
	return "is_ipv4_compat"
}

// Children implements the Expression interface
func (i *IsIPv4Compat) Children() []sql.Expression {
	return []sql.Expression{i.val}
}

// IsNullable implements the Expression interface
func (i IsIPv4Compat) IsNullable() bool {
	return i.val.IsNullable()
}

func (i IsIPv4Compat) String() string {
	return fmt.Sprintf("IS_IPV4_COMPAT(%s)", i.val)
}

func (i IsIPv4Compat) Resolved() bool {
	return i.val.Resolved()
}

func (IsIPv4Compat) Type() sql.Type { return sql.LongText }

func (i IsIPv4Compat) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(i, len(children), 1)
	}
	return NewIsIPv4Compat(i.val), nil
}

// Eval implements the Expression interface
func (i *IsIPv4Compat) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	// Evaluate value
	val, err := i.val.Eval(ctx, row)
	if err != nil {
		return nil, err
	}

	// If null, return nul
	if val == nil {
		return nil, nil
	}

	// Expect to receive a hex encoded string
	switch val.(type) {
	case []byte:
		// Must be of length 16
		if len(val.([]byte)) != 16 {
			return false, nil
		}

		// Check if first 12 bytes are all 0
		for _, b := range val.([]byte)[:12] {
			if b != 0 {
				return false, nil
			}
		}
		return true, nil
	default:
		return false, nil
	}
}

type IsIPv4Mapped struct {
	val sql.Expression
}

var _ sql.FunctionExpression = (*IsIPv4Mapped)(nil)

func NewIsIPv4Mapped(val sql.Expression) sql.Expression {
	return &IsIPv4Mapped{val}
}

func (i *IsIPv4Mapped) FunctionName() string {
	return "is_ipv4_mapped"
}

// Children implements the Expression interface
func (i *IsIPv4Mapped) Children() []sql.Expression {
	return []sql.Expression{i.val}
}

// IsNullable implements the Expression interface
func (i IsIPv4Mapped) IsNullable() bool {
	return i.val.IsNullable()
}

func (i IsIPv4Mapped) String() string {
	return fmt.Sprintf("IS_IPV4_MAPPED(%s)", i.val)
}

func (i IsIPv4Mapped) Resolved() bool {
	return i.val.Resolved()
}

func (IsIPv4Mapped) Type() sql.Type { return sql.LongText }

func (i IsIPv4Mapped) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(i, len(children), 1)
	}
	return NewIsIPv4Mapped(i.val), nil
}

// Eval implements the Expression interface
func (i *IsIPv4Mapped) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	// Evaluate value
	val, err := i.val.Eval(ctx, row)
	if err != nil {
		return nil, err
	}

	// If null, return nul
	if val == nil {
		return nil, nil
	}

	// Expect to receive a hex encoded string
	switch val.(type) {
	case []byte:
		// Must be of length 16
		if len(val.([]byte)) != 16 {
			return false, nil
		}

		// Check if first 10 bytes are all 0
		for _, b := range val.([]byte)[:10] {
			if b != 0 {
				return false, nil
			}
		}

		// Bytes 11 and 12 must be 0xFF
		return val.([]byte)[10] == 0xFF && val.([]byte)[11] == 0xFF, nil
	default:
		return false, nil
	}
}
