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
)

type INET struct {
	val sql.Expression
	ipv4, aton bool
}

var _ sql.FunctionExpression = (*INET)(nil)

func NewINETATON(val sql.Expression) sql.Expression {
	return &INET{val, true, true}
}

func NewINET6ATON(val sql.Expression) sql.Expression {
	return &INET{val, false, true}
}

func NewINETNTOA(val sql.Expression) sql.Expression {
	return &INET{val, true, false}
}

func NewINET6NTOA(val sql.Expression) sql.Expression {
	return &INET{val, false, false}
}

// FunctionName implements sql.FunctionExpression
func (i *INET) FunctionName() string {
	if i.ipv4 {
		if i.aton {
			return "inet_aton"
		} else {
			return "inet_ntoa"
		}
	} else {
		if i.aton {
			return "inet6_aton"
		} else {
			return "inet6_ntoa"
		}
	}
}

// Children implements the Expression interface
func (i *INET) Children() []sql.Expression {
	return []sql.Expression{i.val}
}

// Eval implements the Expression interface
func (i *INET) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	// Evaluate value
	val, err := i.val.Eval(ctx, row)
	if err != nil {
		return nil, err
	}

	if val == nil {
		return nil, nil
	}

	// Convert based on conversion type
	if i.aton {
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

		if i.ipv4 {
			// Expect an IPv4 address
			tmp := ip.To4()
			if tmp == nil {
				// Received invalid IPv4 address (IPv6 address are invalid)
				ctx.Warn(1411, fmt.Sprintf("Incorrect string value: ''%s'' for function %s", val.(string), i.FunctionName()))
				return nil, nil
			}

			// Return IPv4 address as uint32
			ipv4int := binary.BigEndian.Uint32(tmp)
			return ipv4int, nil
		} else {
			// Received IPv4 address
			ipv4 := ip.To4()
			if ipv4 != nil {
				return string(ipv4), nil
			}

			/*
			// Sometimes above check is wrong, check if it really can be treated as IPv4
			tmp := 0
			for _, b := range ip[:12] {
				tmp += int(b)
			}

			// Force it to be IPv4
			if tmp == 0 {
				newipv4 := make(net.IP, 4)
				binary.BigEndian.PutUint32(newipv4, binary.BigEndian.Uint32(ip[12:]))
				return string(newipv4), nil
			}

			*/

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
	} else {
		if i.ipv4 {
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
		} else {
			// TODO: for some reason integers work??

			// Convert into string
			val, err = sql.LongText.Convert(val)
			if err != nil {
				return nil, sql.ErrInvalidType.New(reflect.TypeOf(val).String())
			}

			// There must be a multiple of 4 number of bytes
			tmp := []byte(val.(string))
			if len(tmp) % 4 != 0 {
				ctx.Warn(1411, fmt.Sprintf("Incorrect string value: ''%s'' for function %s", val.(string), i.FunctionName()))
				return nil, nil
			}

			// Create new IPv6
			var ipv6 net.IP = tmp

			// Check to see if it can be treated as IPv4
			tmp1 := 0
			for _, b := range ipv6[:12] {
				tmp1 += int(b)
			}

			// Force it to be IPv4
			if tmp1 == 0 {
				newipv4 := make(net.IP, 4)
				binary.BigEndian.PutUint32(newipv4, binary.BigEndian.Uint32(ipv6[12:]))
				return newipv4.String(), nil
			}

			return ipv6.String(), nil
		}
	}
}

// IsNullable implements the Expression interface
func (i INET) IsNullable() bool {
	return i.val.IsNullable()
}

func (i INET) String() string {
	if i.ipv4 {
		if i.aton {
			return fmt.Sprintf("INET_ATON(%s)", i.val)
		} else {
			return fmt.Sprintf("INET_NTOA(%s)", i.val)
		}
	} else {
		if i.aton {
			return fmt.Sprintf("INET6_ATON(%s)", i.val)
		} else {
			return fmt.Sprintf("INET6_NTOA(%s)", i.val)
		}
	}
}

func (i INET) Resolved() bool {
	return i.val.Resolved()
}

func (INET) Type() sql.Type { return sql.LongText }

func (i INET) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(i, len(children), 1)
	}
	if i.ipv4 {
		if i.aton {
			return NewINETATON(i.val), nil
		} else {
			return NewINETNTOA(i.val), nil
		}
	} else {
		if i.aton {
			return NewINET6ATON(i.val), nil
		} else {
			return NewINET6NTOA(i.val), nil
		}
	}
}