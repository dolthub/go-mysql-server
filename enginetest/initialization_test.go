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

package enginetest

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIsCreateIndex(t *testing.T) {
	for _, tt := range []struct {
		stmt string
		exp  bool
	}{
		{"create index mytable_i_s on mytable (i,s)", true},
		{"CREATE INDEX `idx_si` ON mytable (`s`,`i`)", true},
		{"  create\tunique index u on t (a)", true},
		{"create fulltext index f on t (a)", true},
		{"create vector index v on t (a)", true},
		{"create table t (i int, index (i))", false},
		{"create unique_table_named_index (i int)", false},
		{"alter table t add index (i)", false},
		{"insert into t values ('create index')", false},
		{"create", false},
		{"", false},
	} {
		t.Run(tt.stmt, func(t *testing.T) {
			assert.Equal(t, tt.exp, isCreateIndex(tt.stmt))
		})
	}
}
