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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/expression/function"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/types"
)

func TestCheckConstraintNonDeterministicFunctions(t *testing.T) {
	tests := []struct {
		name      string
		overrides sql.EngineOverrides
		valid     bool
	}{
		{
			name:  "MySQL compatibility",
			valid: false,
		},
		{
			name:      "PostgreSQL compatibility",
			overrides: sql.EngineOverrides{PermitNonDeterministicCheckFunctions: true},
			valid:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := sql.NewEmptyContext()
			a := NewDefault(sql.NewDatabaseProvider())
			a.Overrides = tt.overrides
			check := &plan.CreateCheck{
				Check: &sql.CheckConstraint{
					Name:     "chk",
					Expr:     expression.NewNot(expression.NewEquals(function.NewUUIDFunc(ctx), expression.NewLiteral("", types.Text))),
					Enforced: true,
				},
			}

			_, _, err := validateCheckConstraints(ctx, a, check, nil, nil, nil)
			if tt.valid {
				require.NoError(t, err)
			} else {
				require.True(t, sql.ErrInvalidConstraintFunctionNotSupported.Is(err))
			}
		})
	}
}
