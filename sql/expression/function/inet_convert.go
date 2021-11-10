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
	"math/big"
	"net"
	"reflect"

	"github.com/dolthub/go-mysql-server/sql"
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

	// Convert based on conversion type
	if i.aton {
		// Convert val into string
		val, err = sql.LongText.Convert(val)
		if err != nil {
			return nil, sql.ErrInvalidType.New(reflect.TypeOf(val).String())
		}

		// Parse IP address
		ip := net.ParseIP(val.(string))
		if ip == nil {
			// TODO: return error Incorrect string value <val> for function <func_name>
			return nil, sql.ErrInvalidType.New(reflect.TypeOf(val).String())
		}

		if i.ipv4 {
			tmp := ip.To4()
			if tmp == nil {
				// If you try to parse IPv6 as IPv4
				// TODO: return Warning incorrect string value
				return nil, sql.ErrInvalidType.New(reflect.TypeOf(val).String())
			}

			ipv4int := binary.BigEndian.Uint32(tmp)
			return ipv4int, nil
		} else {
			// If possible to do it as IPv4, do that
			tmp := ip.To4()
			if tmp != nil {
				ipv4int := binary.BigEndian.Uint32(tmp)
				return ipv4int, nil
			}

			tmp = ip.To16()
			if tmp == nil {
				// TODO: return Warning incorrect string value
				return nil, sql.ErrInvalidType.New(reflect.TypeOf(val).String())
			}

			ipv6int := big.NewInt(0)
			ipv6int.SetBytes(tmp)
			return ipv6int, nil
		}
	} else {
		if i.ipv4 {
			// Convert val into int
			val, err = sql.Int32.Convert(val)
			if err != nil {
				return nil, sql.ErrInvalidType.New(reflect.TypeOf(val).String())
			}

			// Create new IPv4, and fill with val
			ipv4 := make(net.IP, 4)
			binary.BigEndian.PutUint32(ipv4, uint32(val.(int32)))

			return ipv4.String(), nil
		} else {
			// Convert val into string then into big int
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

			// Create new IPv6
			var ipv6 net.IP = ipv6int.Bytes()

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