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

	"github.com/gabereiser/go-mysql-server/sql"
	"github.com/gabereiser/go-mysql-server/sql/types"

	"github.com/stretchr/testify/require"
)

func TestIsNull(t *testing.T) {
	require := require.New(t)

	get0 := NewGetField(0, types.Text, "col1", true)
	e := NewIsNull(get0)
	require.Equal(types.Boolean, e.Type())
	require.Equal(false, e.IsNullable())
	require.Equal(true, eval(t, e, sql.NewRow(nil)))
	require.Equal(false, eval(t, e, sql.NewRow("")))
}
