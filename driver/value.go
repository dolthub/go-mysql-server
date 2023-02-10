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

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
)

// ErrUnsupportedType is returned when a query argument of an unsupported type is passed to a statement
var ErrUnsupportedType = errors.New("unsupported type")

func valueToExpr(v driver.Value) (sql.Expression, error) {
	if v == nil {
		return expression.NewLiteral(nil, sql.Null), nil
	}

	var typ sql.Type
	var err error
	switch v := v.(type) {
	case int64:
		typ = sql.Int64
	case float64:
		typ = sql.Float64
	case bool:
		typ = sql.Boolean
	case []byte:
		typ, err = sql.CreateStringWithDefaults(sqltypes.Blob, int64(len(v)))
	case string:
		typ, err = sql.CreateStringWithDefaults(sqltypes.Text, int64(len(v)))
	case time.Time:
		typ = sql.Datetime
	default:
		return nil, fmt.Errorf("%w: %T", ErrUnsupportedType, v)
	}
	if err != nil {
		return nil, err
	}

	c, err := typ.Convert(v)
	if err != nil {
		return nil, err
	}
	return expression.NewLiteral(c, typ), nil
}

func valuesToBindings(v []driver.Value) (map[string]sql.Expression, error) {
	if len(v) == 0 {
		return nil, nil
	}

	b := map[string]sql.Expression{}

	var err error
	for i, v := range v {
		b[strconv.FormatInt(int64(i), 10)], err = valueToExpr(v)
		if err != nil {
			return nil, err
		}
	}

	return b, nil
}

func namedValuesToBindings(v []driver.NamedValue) (map[string]sql.Expression, error) {
	if len(v) == 0 {
		return nil, nil
	}

	b := map[string]sql.Expression{}

	var err error
	for _, v := range v {
		name := v.Name
		if name == "" {
			name = "v" + strconv.FormatInt(int64(v.Ordinal), 10)
		}

		b[name], err = valueToExpr(v.Value)
		if err != nil {
			return nil, err
		}
	}

	return b, nil
}
