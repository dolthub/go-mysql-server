// Copyright 2026 Dolthub, Inc.
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
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/transform"
)

func TestBatchStopsWhenCanceled(t *testing.T) {
	ctx, cancel := sql.NewEmptyContext().NewSubContext()
	defer cancel()

	var ran []RuleId
	rule := func(id RuleId, f func()) Rule {
		return Rule{Id: id, Apply: func(*sql.Context, *Analyzer, sql.Node, *plan.Scope, RuleSelector, *sql.QueryFlags) (sql.Node, transform.TreeIdentity, error) {
			ran = append(ran, id)
			f()
			return plan.NewShowStatus(false), transform.NewTree, nil
		}}
	}
	b := &Batch{
		Iterations: 1,
		Rules: []Rule{
			rule(validateResolvedId, cancel),
			rule(validateOrderById, func() {}),
		},
	}

	_, _, err := b.Eval(ctx, NewDefault(nil), plan.NewShowStatus(true), nil, func(RuleId) bool { return true }, nil)
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, []RuleId{validateResolvedId}, ran)
}
