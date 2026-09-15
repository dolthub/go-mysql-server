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

package rowexec

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/binlogreplication"
	"github.com/dolthub/go-mysql-server/sql/plan"
)

// statusReplicaController supplies deterministic status while embedding the unused controller methods.
type statusReplicaController struct {
	binlogreplication.BinlogReplicaController
	status *binlogreplication.ReplicaStatus
}

// GetReplicaStatus returns the configured test status.
func (c statusReplicaController) GetReplicaStatus(*sql.Context) (*binlogreplication.ReplicaStatus, error) {
	return c.status, nil
}

// TestBuildShowReplicaStatusWildcardFilters protects the wildcard filter column positions and formatting.
func TestBuildShowReplicaStatusWildcardFilters(t *testing.T) {
	controller := statusReplicaController{status: &binlogreplication.ReplicaStatus{
		ReplicateDoTables:         []string{"db.exact"},
		ReplicateIgnoreTables:     []string{"db.ignored"},
		ReplicateWildDoTables:     []string{"db.a%", "other._"},
		ReplicateWildIgnoreTables: []string{`db.literal\%`},
	}}
	node := plan.NewShowReplicaStatus().WithBinlogReplicaController(controller).(*plan.ShowReplicaStatus)
	ctx := sql.NewEmptyContext()

	iter, err := (&BaseBuilder{}).buildShowReplicaStatus(ctx, node, nil)
	require.NoError(t, err)
	row, err := iter.Next(ctx)
	require.NoError(t, err)
	require.Equal(t, "db.exact", row[14])
	require.Equal(t, "db.ignored", row[15])
	require.Equal(t, "db.a%,other._", row[16])
	require.Equal(t, `db.literal\%`, row[17])
}
