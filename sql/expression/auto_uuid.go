// Copyright 2024 Dolthub, Inc.
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

package expression

import (
	"fmt"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
	"github.com/dolthub/vitess/go/sqltypes"
)

// AutoUuid is an expression that captures an automatically generated UUID value and stores it in the session for
// later retrieval.
type AutoUuid struct {
	UnaryExpression
	uuidCol   *sql.Column
	foundUuid bool
}

var _ sql.Expression = (*AutoUuid)(nil)
var _ sql.CollationCoercible = (*AutoUuid)(nil)

// NewAutoUuid creates a new AutoUuid expression.
func NewAutoUuid(_ *sql.Context, col *sql.Column, child sql.Expression) *AutoUuid {
	return &AutoUuid{
		UnaryExpression: UnaryExpression{Child: child},
		uuidCol:         col,
	}
}

// IsNullable implements the Expression interface.
func (au *AutoUuid) IsNullable() bool {
	return false
}

// Type implements the Expression interface.
func (au *AutoUuid) Type() sql.Type {
	return types.MustCreateString(sqltypes.Char, 36, sql.Collation_Default)
}

// CollationCoercibility implements the interface sql.CollationCoercible.
func (au *AutoUuid) CollationCoercibility(ctx *sql.Context) (collation sql.CollationID, coercibility byte) {
	return sql.GetCoercibility(ctx, au.Child)
}

// Eval implements the Expression interface.
func (au *AutoUuid) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	// get value provided by INSERT
	given, err := au.Child.Eval(ctx, row)
	if err != nil {
		return nil, err
	}

	// TODO: Technically... setting this here means that another call to last_insert_uuid() in the same statement would
	//       read this value too early. Test this behaviro with MySQL and make sure that's how MySQL works. Then,
	//       add a test for that edge case. To fix, we could set a PENDING_LAST_INSERT_UUID or something similar, then
	//       as part of insertIter execution phase, we could move it from PENDING to LAST_INSERT_UUID
	if !au.foundUuid {
		uuidValue, ok := given.(string)
		if !ok {
			// This should never happen – AutoUuid should only ever be placed directly above a UUID function,
			// so the result from eval'ing its child should *always* be a string.
			return nil, fmt.Errorf("unexpected type for UUID value: %T", given)
		}

		ctx.Session.SetLastQueryInfoString(sql.LastInsertUuid, uuidValue)
		au.foundUuid = true
	}

	return given, nil
}

func (au *AutoUuid) String() string {
	return fmt.Sprintf("AutoUuid(%s)", au.Child.String())
}

// WithChildren implements the Expression interface.
func (au *AutoUuid) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(au, len(children), 1)
	}
	return &AutoUuid{
		UnaryExpression: UnaryExpression{Child: children[0]},
		uuidCol:         au.uuidCol,
		foundUuid:       au.foundUuid,
	}, nil
}

// Children implements the Expression interface.
func (au *AutoUuid) Children() []sql.Expression {
	return []sql.Expression{au.Child}
}
