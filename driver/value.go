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
	"strconv"
	"time"

	"github.com/dolthub/vitess/go/sqltypes"
	"github.com/dolthub/vitess/go/vt/sqlparser"
)

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
