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

package analyzer

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
)

func TestTrackProcessSubquery(t *testing.T) {
	require := require.New(t)
	rule := getRuleFrom(OnceAfterAll, TrackProcessId)
	a := NewDefault(sql.NewDatabaseProvider())

	db := memory.NewDatabase("db")
	pro := memory.NewDBProvider(db)
	ctx := newContext(pro)

	node := plan.NewProject(
		nil,
		plan.NewSubqueryAlias("f", "",
			plan.NewQueryProcess(
				plan.NewResolvedTable(memory.NewTable(db, "foo", sql.PrimaryKeySchema{}, nil), nil, nil),
				nil,
			),
		),
	)

	result, _, err := rule.Apply(ctx, a, node, nil, DefaultRuleSelector)
	require.NoError(err)

	expectedChild := plan.NewProject(
		nil,
		plan.NewSubqueryAlias("f", "",
			plan.NewResolvedTable(memory.NewTable(db, "foo", sql.PrimaryKeySchema{}, nil), nil, nil),
		),
	)

	proc, ok := result.(*plan.QueryProcess)
	require.True(ok)
	require.Equal(expectedChild, proc.Child())
}

// wrapper around sql.Table to make it not indexable
type table struct {
	sql.Table
}

var _ sql.PartitionCounter = (*table)(nil)

func (t *table) PartitionCount(ctx *sql.Context) (int64, error) {
	return t.Table.(sql.PartitionCounter).PartitionCount(ctx)
}
