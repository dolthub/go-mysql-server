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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/types"
)

func TestGetFormat(t *testing.T) {
	testCases := []struct {
		left  sql.Expression
		right sql.Expression
		exp   interface{}
	}{
		{
			left:  expression.NewLiteral(nil, types.Null),
			right: expression.NewLiteral(nil, types.Null),
			exp:   nil,
		},
		{
			left:  expression.NewLiteral("date", types.Text),
			right: expression.NewLiteral(nil, types.Null),
			exp:   nil,
		},
		{
			left:  expression.NewLiteral(nil, types.Null),
			right: expression.NewLiteral("us", types.Text),
			exp:   nil,
		},
		{
			left:  expression.NewLiteral("dATeTiMe", types.Text),
			right: expression.NewLiteral("InTeRnAl", types.Text),
			exp:   "%Y%m%d%H%i%s",
		},

		{
			left:  expression.NewLiteral("date", types.Text),
			right: expression.NewLiteral("usa", types.Text),
			exp:   "%m.%d.%Y",
		},
		{
			left:  expression.NewLiteral("date", types.Text),
			right: expression.NewLiteral("jis", types.Text),
			exp:   "%Y-%m-%d",
		},
		{
			left:  expression.NewLiteral("date", types.Text),
			right: expression.NewLiteral("iso", types.Text),
			exp:   "%Y-%m-%d",
		},
		{
			left:  expression.NewLiteral("date", types.Text),
			right: expression.NewLiteral("eur", types.Text),
			exp:   "%d.%m.%Y",
		},
		{
			left:  expression.NewLiteral("date", types.Text),
			right: expression.NewLiteral("internal", types.Text),
			exp:   "%Y%m%d",
		},

		{
			left:  expression.NewLiteral("datetime", types.Text),
			right: expression.NewLiteral("usa", types.Text),
			exp:   "%Y-%m-%d %H.%i.%s",
		},
		{
			left:  expression.NewLiteral("datetime", types.Text),
			right: expression.NewLiteral("jis", types.Text),
			exp:   "%Y-%m-%d %H:%i:%s",
		},
		{
			left:  expression.NewLiteral("datetime", types.Text),
			right: expression.NewLiteral("iso", types.Text),
			exp:   "%Y-%m-%d %H:%i:%s",
		},
		{
			left:  expression.NewLiteral("datetime", types.Text),
			right: expression.NewLiteral("eur", types.Text),
			exp:   "%Y-%m-%d %H.%i.%s",
		},
		{
			left:  expression.NewLiteral("datetime", types.Text),
			right: expression.NewLiteral("internal", types.Text),
			exp:   "%Y%m%d%H%i%s",
		},

		{
			left:  expression.NewLiteral("time", types.Text),
			right: expression.NewLiteral("usa", types.Text),
			exp:   "%h:%i:%s %p",
		},
		{
			left:  expression.NewLiteral("time", types.Text),
			right: expression.NewLiteral("jis", types.Text),
			exp:   "%H:%i:%s",
		},
		{
			left:  expression.NewLiteral("time", types.Text),
			right: expression.NewLiteral("iso", types.Text),
			exp:   "%H:%i:%s",
		},
		{
			left:  expression.NewLiteral("time", types.Text),
			right: expression.NewLiteral("eur", types.Text),
			exp:   "%H.%i.%s",
		},
		{
			left:  expression.NewLiteral("time", types.Text),
			right: expression.NewLiteral("internal", types.Text),
			exp:   "%H%i%s",
		},

		{
			left:  expression.NewLiteral("timestamp", types.Text),
			right: expression.NewLiteral("usa", types.Text),
			exp:   "%Y-%m-%d %H.%i.%s",
		},
		{
			left:  expression.NewLiteral("timestamp", types.Text),
			right: expression.NewLiteral("jis", types.Text),
			exp:   "%Y-%m-%d %H:%i:%s",
		},
		{
			left:  expression.NewLiteral("timestamp", types.Text),
			right: expression.NewLiteral("iso", types.Text),
			exp:   "%Y-%m-%d %H:%i:%s",
		},
		{
			left:  expression.NewLiteral("timestamp", types.Text),
			right: expression.NewLiteral("eur", types.Text),
			exp:   "%Y-%m-%d %H.%i.%s",
		},
		{
			left:  expression.NewLiteral("timestamp", types.Text),
			right: expression.NewLiteral("internal", types.Text),
			exp:   "%Y%m%d%H%i%s",
		},
	}

	for _, tt := range testCases {
		t.Run(fmt.Sprintf("%s %s", tt.left.String(), tt.right.String()), func(t *testing.T) {
			require := require.New(t)
			ctx := sql.NewEmptyContext()
			f := NewGetFormat(tt.left, tt.right)
			out, err := f.Eval(ctx, nil)
			require.NoError(err)
			require.Equal(tt.exp, out)
		})
	}
}
