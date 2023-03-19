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

package sql_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/gabereiser/go-mysql-server/sql"
	"github.com/gabereiser/go-mysql-server/sql/expression"
	"github.com/gabereiser/go-mysql-server/sql/types"
)

var conditions = []struct {
	evaluated bool
	value     interface{}
	t         sql.Type
}{
	{true, int16(1), types.Int16},
	{false, int16(0), types.Int16},
	{true, int32(1), types.Int32},
	{false, int32(0), types.Int32},
	{true, int(1), types.Int64},
	{false, int(0), types.Int64},
	{true, float32(1), types.Float32},
	{true, float64(1), types.Float64},
	{false, float32(0), types.Float32},
	{false, float64(0), types.Float64},
	{true, float32(0.5), types.Float32},
	{true, float64(0.5), types.Float64},
	{true, "1", types.LongText},
	{false, "0", types.LongText},
	{false, "foo", types.LongText},
	{false, "0.5", types.LongText},
	{false, time.Duration(0), types.Timestamp},
	{true, time.Duration(1), types.Timestamp},
	{false, false, types.Boolean},
	{true, true, types.Boolean},
}

func TestEvaluateCondition(t *testing.T) {
	for _, v := range conditions {
		t.Run(fmt.Sprint(v.value, " evaluated to ", v.evaluated, " type ", v.t), func(t *testing.T) {
			require := require.New(t)
			b, err := sql.EvaluateCondition(sql.NewEmptyContext(), expression.NewLiteral(v.value, v.t), sql.NewRow())
			require.NoError(err)
			require.Equal(v.evaluated, b)
		})
	}
}
