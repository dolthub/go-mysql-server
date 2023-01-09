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

package expression

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
)

func TestNot(t *testing.T) {
	require := require.New(t)

	e := NewNot(NewGetField(0, types.Text, "foo", true))
	require.False(eval(t, e, sql.NewRow(true)).(bool))
	require.True(eval(t, e, sql.NewRow(false)).(bool))
	require.Nil(eval(t, e, sql.NewRow(nil)))
	require.False(eval(t, e, sql.NewRow(1)).(bool))
	require.True(eval(t, e, sql.NewRow(0)).(bool))
	require.False(eval(t, e, sql.NewRow(time.Now())).(bool))
	require.False(eval(t, e, sql.NewRow(time.Second)).(bool))
	require.True(eval(t, e, sql.NewRow("any string always false")).(bool))
}
