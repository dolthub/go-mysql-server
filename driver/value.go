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

package driver

import (
	"database/sql/driver"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/dolthub/vitess/go/sqltypes"
	"github.com/dolthub/vitess/go/vt/sqlparser"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/types"
)

// ErrUnsupportedType is returned when a query argument of an unsupported type is passed to a statement
var ErrUnsupportedType = errors.New("unsupported type")

func valueToExpr(v driver.Value) (sql.Expression, error) {
	if v == nil {
		return expression.NewLiteral(nil, types.Null), nil
	}

	var typ sql.Type
	var err error
	switch v := v.(type) {
	case int64:
		typ = types.Int64
	case float64:
		typ = types.Float64
	case bool:
		typ = types.Boolean
	case []byte:
		typ, err = types.CreateBinary(sqltypes.Blob, int64(len(v)))
	case string:
		typ, err = types.CreateStringWithDefaults(sqltypes.Text, int64(len(v)))
	case time.Time:
		typ = types.Datetime
	default:
		return nil, fmt.Errorf("%w: %T", ErrUnsupportedType, v)
	}
	if err != nil {
		return nil, err
	}

	c, _, err := typ.Convert(ctx, v)
	if err != nil {
		return nil, err
	}
	return expression.NewLiteral(c, typ), nil
}

func valuesToBindings(vals []driver.Value) (map[string]sqlparser.Expr, error) {
	if len(vals) == 0 {
		return nil, nil
	}

	b := map[string]sqlparser.Expr{}

	var err error
	for i, val := range vals {
		b[strconv.FormatInt(int64(i), 10)], err = valToBinding(val)
		if err != nil {
			return nil, err
		}
	}

	return b, nil
}

func namedValuesToBindings(namedVals []driver.NamedValue) (map[string]sqlparser.Expr, error) {
	if len(namedVals) == 0 {
		return nil, nil
	}

	b := map[string]sqlparser.Expr{}
	var err error
	for _, namedVal := range namedVals {
		name := namedVal.Name
		if name == "" {
			name = "v" + strconv.FormatInt(int64(namedVal.Ordinal), 10)
		}

		b[name], err = valToBinding(namedVal.Value)
		if err != nil {
			return nil, err
		}
	}

	return b, nil
}

func valToBinding(val driver.Value) (sqlparser.Expr, error) {
	if t, ok := val.(time.Time); ok {
		val = t.Format(time.RFC3339Nano)
	}
	bv, err := sqltypes.BuildBindVariable(val)
	if err != nil {
		return nil, err
	}
	v, err := sqltypes.BindVariableToValue(bv)
	if err != nil {
		return nil, err
	}
	return sqlparser.ExprFromValue(v)
}
