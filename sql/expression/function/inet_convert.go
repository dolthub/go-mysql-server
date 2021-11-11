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
	"encoding/binary"
	"fmt"
	"github.com/dolthub/go-mysql-server/sql"
	"net"
	"reflect"
	"strings"
)

type INETATON struct {
	val sql.Expression
}

var _ sql.FunctionExpression = (*INETATON)(nil)

func NewINETATON(val sql.Expression) sql.Expression {
	return &INETATON{val}
}

func (i *INETATON) FunctionName() string {
	return "inet_aton"
}

// IsNullable implements the Expression interface
func (i INETATON) IsNullable() bool {
	return i.val.IsNullable()
}

func (i INETATON) String() string {
	return fmt.Sprintf("INET_ATON(%s)", i.val)
}

func (i INETATON) Resolved() bool {
	return i.val.Resolved()
}

func (INETATON) Type() sql.Type {
	return sql.LongText
}

func (i *INETATON) Children() []sql.Expression {
	return []sql.Expression{i.val}
}

func (i INETATON) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(i, len(children), 1)
	}
	return NewINETATON(i.val), nil
}

func (i *INETATON) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	// Evaluate value
	val, err := i.val.Eval(ctx, row)
	if err != nil {
		return nil, err
	}

	// Return null if given null
	if val == nil {
		return nil, nil
	}

	// Expect to receive an IP address, so convert val into string
	val, err = sql.LongText.Convert(val)
	if err != nil {
		return nil, sql.ErrInvalidType.New(reflect.TypeOf(val).String())
	}

	// Parse IP address
	ip := net.ParseIP(val.(string))
	if ip == nil {
		// Failed to Parse IP correctly
		ctx.Warn(1411, fmt.Sprintf("Incorrect string value: ''%s'' for function %s", val.(string), i.FunctionName()))
		return nil, nil
	}

	// Expect an IPv4 address
	ipv4 := ip.To4()
	if ipv4 == nil {
		// Received invalid IPv4 address (IPv6 address are invalid)
		ctx.Warn(1411, fmt.Sprintf("Incorrect string value: ''%s'' for function %s", val.(string), i.FunctionName()))
		return nil, nil
	}

	// Return IPv4 address as uint32
	ipv4int := binary.BigEndian.Uint32(ipv4)
	return ipv4int, nil
}


type INET6ATON struct {
	val sql.Expression
}

var _ sql.FunctionExpression = (*INET6ATON)(nil)

func NewINET6ATON(val sql.Expression) sql.Expression {
	return &INET6ATON{val}
}

func (i *INET6ATON) FunctionName() string {
	return "inet6_aton"
}

// IsNullable implements the Expression interface
func (i INET6ATON) IsNullable() bool {
	return i.val.IsNullable()
}

func (i INET6ATON) String() string {
	return fmt.Sprintf("INET6_ATON(%s)", i.val)
}

func (i INET6ATON) Resolved() bool {
	return i.val.Resolved()
}

func (INET6ATON) Type() sql.Type {
	return sql.LongText
}

func (i *INET6ATON) Children() []sql.Expression {
	return []sql.Expression{i.val}
}

func (i INET6ATON) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(i, len(children), 1)
	}
	return NewINET6ATON(i.val), nil
}

func (i *INET6ATON) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	// Evaluate value
	val, err := i.val.Eval(ctx, row)
	if err != nil {
		return nil, err
	}

	// Return null if given null
	if val == nil {
		return nil, nil
	}

	// Parse IP address
	ip := net.ParseIP(val.(string))
	if ip == nil {
		// Failed to Parse IP correctly
		ctx.Warn(1411, fmt.Sprintf("Incorrect string value: ''%s'' for function %s", val.(string), i.FunctionName()))
		return nil, nil
	}

	// if it doesn't contain colons, treat it as ipv4
	if strings.Count(val.(string), ":") < 2 {
		ipv4 := ip.To4()
		return string(ipv4), nil
	}

	// Received IPv6 address
	ipv6 := ip.To16()
	if ipv6 == nil {
		// Invalid IPv6 address
		ctx.Warn(1411, fmt.Sprintf("Incorrect string value: ''%s'' for function %s", val.(string), i.FunctionName()))
		return nil, nil
	}

	// Return as string
	return string(ipv6), nil
}


type INETNTOA struct {
	val sql.Expression
}

var _ sql.FunctionExpression = (*INETNTOA)(nil)

func NewINETNTOA(val sql.Expression) sql.Expression {
	return &INETNTOA{val}
}

func (i *INETNTOA) FunctionName() string {
	return "inet_ntoa"
}

// IsNullable implements the Expression interface
func (i INETNTOA) IsNullable() bool {
	return i.val.IsNullable()
}

func (i INETNTOA) String() string {
	return fmt.Sprintf("INET_NTOA(%s)", i.val)
}

func (i INETNTOA) Resolved() bool {
	return i.val.Resolved()
}

func (INETNTOA) Type() sql.Type {
	return sql.LongText
}

func (i *INETNTOA) Children() []sql.Expression {
	return []sql.Expression{i.val}
}

func (i INETNTOA) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(i, len(children), 1)
	}
	return NewINETNTOA(i.val), nil
}

func (i *INETNTOA) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	// Evaluate value
	val, err := i.val.Eval(ctx, row)
	if err != nil {
		return nil, err
	}

	// Return null if given null
	if val == nil {
		return nil, nil
	}

	// Convert val into int
	ipv4int, err := sql.Int32.Convert(val)
	if ipv4int != nil && err != nil {
		return nil, sql.ErrInvalidType.New(reflect.TypeOf(val).String())
	}

	// Received a hex string instead of int
	if ipv4int == nil {
		// Create new IPv4
		var ipv4 net.IP = []byte{0, 0, 0, 0}
		return ipv4.String(), nil
	}

	// Create new IPv4, and fill with val
	ipv4 := make(net.IP, 4)
	binary.BigEndian.PutUint32(ipv4, uint32(ipv4int.(int32))) // TODO: can't cast directly to uint32, any other way?

	return ipv4.String(), nil
}


type INET6NTOA struct {
	val sql.Expression
}

var _ sql.FunctionExpression = (*INET6NTOA)(nil)

func NewINET6NTOA(val sql.Expression) sql.Expression {
	return &INET6NTOA{val}
}

func (i *INET6NTOA) FunctionName() string {
	return "inet6_ntoa"
}

// IsNullable implements the Expression interface
func (i INET6NTOA) IsNullable() bool {
	return i.val.IsNullable()
}

func (i INET6NTOA) String() string {
	return fmt.Sprintf("INET6_NTOA(%s)", i.val)
}

func (i INET6NTOA) Resolved() bool {
	return i.val.Resolved()
}

func (INET6NTOA) Type() sql.Type {
	return sql.LongText
}

func (i *INET6NTOA) Children() []sql.Expression {
	return []sql.Expression{i.val}
}

func (i INET6NTOA) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(i, len(children), 1)
	}
	return NewINET6NTOA(i.val), nil
}

func (i *INET6NTOA) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	// Evaluate value
	val, err := i.val.Eval(ctx, row)
	if err != nil {
		return nil, err
	}

	// Return null if given null
	if val == nil {
		return nil, nil
	}

	// Only convert if received string as input
	switch val.(type) {
	case string:
		// TODO change this and others to expect byte slices
		tmp := []byte(val.(string))

		// Exactly 4 bytes, treat as IPv4 address
		if len(tmp) == 4 {
			var ipv4 net.IP = tmp
			return ipv4.String(), nil
		}

		// There must be exactly 4 or 16 bytes (len == 4 satisfied above)
		if len(tmp) != 16 {
			ctx.Warn(1411, fmt.Sprintf("Incorrect string value: ''%s'' for function %s", val.(string), i.FunctionName()))
			return nil, nil
		}

		// Check to see if it should be printed as IPv6; non-zero within first 10 bytes
		for _, b := range tmp[:10] {
			if b != 0 {
				// Create new IPv6
				var ipv6 net.IP = tmp
				return ipv6.String(), nil
			}
		}

		// IPv4-compatible (12 bytes of 0x00)
		if tmp[10] == 0 && tmp[11] == 0 && (tmp[12] != 0 || tmp[13] != 0) {
			var ipv4 net.IP = tmp[12:]
			return "::" + ipv4.String(), nil
		}

		// IPv4-mapped (10 bytes of 0x00 followed by 2 bytes of 0xFF)
		if tmp[10] == 0xFF && tmp[11] == 0xFF {
			var ipv4 net.IP = tmp[12:]
			return "::ffff:" + ipv4.String(), nil
		}

		// Print as IPv6 by default
		var ipv6 net.IP = tmp
		return ipv6.String(), nil
	default:
		ctx.Warn(1411, fmt.Sprintf("Incorrect string value: ''%v'' for function %s", val, i.FunctionName()))
		return nil, nil
	}
}