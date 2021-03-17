// Copyright 2021 Dolthub, Inc.
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
	"github.com/dolthub/vitess/go/sqltypes"
	"github.com/google/uuid"
)

// UUID()
//
// Returns a Universal Unique Identifier (UUID) generated according to RFC 4122, “A Universally Unique IDentifier (UUID)
// URN Namespace” (http://www.ietf.org/rfc/rfc4122.txt). A UUID is designed as a number that is globally unique in space
// and time. Two calls to UUID() are expected to generate two different values, even if these calls are performed on two
// separate devices not connected to each other.
//
// Warning Although UUID() values are intended to be unique, they are not necessarily unguessable or unpredictable.
// If unpredictability is required, UUID values should be generated some other way. UUID() returns a value that conforms
// to UUID version 1 as described in RFC 4122. The value is a 128-bit number represented as a utf8 string of five
// hexadecimal numbers in aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee format:

// The first three numbers are generated from the low, middle, and high parts of a timestamp. The high part also includes
// the UUID version number.
//
// The fourth number preserves temporal uniqueness in case the timestamp value loses monotonicity
// (for example, due to daylight saving time).
//
// The fifth number is an IEEE 802 node number that provides spatial uniqueness. A random number is substituted if the
// latter is not available (for example, because the host device has no Ethernet card, or it is unknown how to find the
// hardware address of an interface on the host operating system). In this case, spatial uniqueness cannot be guaranteed.
// Nevertheless, a collision should have very low probability.
//
// The MAC address of an interface is taken into account only on FreeBSD, Linux, and Windows. On other operating systems,
// MySQL uses a randomly generated 48-bit number.
// https://dev.mysql.com/doc/refman/8.0/en/miscellaneous-functions.html#function_uuid

type UUIDFunc struct {}

var _ sql.FunctionExpression = &UUIDFunc{}

func NewUUIDFunc() sql.Expression{
	return UUIDFunc{}
}

func (U UUIDFunc) String() string {
	return "UUID()"
}

func (U UUIDFunc) Type() sql.Type {
	return sql.MustCreateStringWithDefaults(sqltypes.VarChar, 36)
}

func (U UUIDFunc) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	return uuid.New().String(), nil
}

func (U UUIDFunc) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 0 {
		return nil, sql.ErrInvalidChildrenNumber.New(U, len(children), 0)
	}

	return &UUIDFunc{}, nil
}

func (U UUIDFunc) FunctionName() string {
	return "uuid"
}

func (U UUIDFunc) Resolved() bool {
	return true
}

// Children returns the children expressions of this expression.
func (U UUIDFunc) Children() []sql.Expression {
	return nil
}

// IsNullable returns whether the expression can be null.
func (U UUIDFunc) IsNullable() bool {
	return false
}

// IS_UUID(string_uuid)
//
// Returns 1 if the argument is a valid string-format UUID, 0 if the argument is not a valid UUID, and NULL if the
// argument is NULL.
//
// “Valid” means that the value is in a format that can be parsed. That is, it has the correct length and contains only
// the permitted characters (hexadecimal digits in any lettercase and, optionally, dashes and curly braces).

type IsUUID struct {
	inputStr sql.Expression
}

var _ sql.FunctionExpression = &IsUUID{}

func NewIsUUID(arg sql.Expression) sql.Expression {
	return IsUUID{inputStr: arg}
}

func (U IsUUID) String() string {
	return "IS_UUID()"
}

func (U IsUUID) Type() sql.Type {
	return sql.Int8
}

func (U IsUUID) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	str, err := U.inputStr.Eval(ctx, row)
	if err != nil {
		return 0, err
	}

	switch str := str.(type) {
	case string:
		_, err := uuid.Parse(str)
		if err != nil {
			return int8(0), nil
		}

		return int8(1), nil
	default:
		return 0, nil
	}
}

func (U IsUUID) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(U, len(children), 1)
	}

	return &IsUUID{inputStr: children[0]}, nil
}

func (U IsUUID) FunctionName() string {
	return "is_uuid"
}

func (U IsUUID) Resolved() bool {
	return true
}

// Children returns the children expressions of this expression.
func (U IsUUID) Children() []sql.Expression {
	return []sql.Expression{U.inputStr}
}

// IsNullable returns whether the expression can be null.
func (U IsUUID) IsNullable() bool {
	return false
}
