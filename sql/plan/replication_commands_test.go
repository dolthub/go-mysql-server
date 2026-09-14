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

package plan

import (
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/binlogreplication"
	"github.com/stretchr/testify/require"
)

// TestReplicationCommandString verifies native replication option values retain readable plan formatting.
func TestReplicationCommandString(t *testing.T) {
	source := NewChangeReplicationSource([]binlogreplication.ReplicationOption{
		*binlogreplication.NewReplicationOption("SOURCE_HOST", "primary"),
		*binlogreplication.NewReplicationOption("SOURCE_PORT", 3306),
	})
	require.Equal(t, "CHANGE REPLICATION SOURCE TO SOURCE_HOST = primary, SOURCE_PORT = 3306", source.String())

	filter := NewChangeReplicationFilter([]binlogreplication.ReplicationOption{
		*binlogreplication.NewReplicationOption("REPLICATE_DO_TABLE", []sql.UnresolvedTable{
			NewUnresolvedTable("orders", "sales"),
			NewUnresolvedTable("events", "analytics"),
		}),
		*binlogreplication.NewReplicationOption("REPLICATE_WILD_IGNORE_TABLE", []string{"archive.%", "tmp._"}),
	})
	require.Equal(t, "CHANGE REPLICATION FILTER REPLICATE_DO_TABLE = sales.orders, analytics.events, REPLICATE_WILD_IGNORE_TABLE = archive.%, tmp._", filter.String())
}
