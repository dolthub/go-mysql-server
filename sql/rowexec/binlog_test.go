// Copyright 2025 Dolthub, Inc.
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
	"encoding/base64"
	"encoding/binary"
	"io"
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/binlogreplication"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/types"
	"github.com/dolthub/vitess/go/mysql"
	"github.com/stretchr/testify/require"
)

func TestBuildBinlog_InvalidBase64(t *testing.T) {
	builder := &BaseBuilder{}
	ctx := sql.NewEmptyContext()

	binlogNode := plan.NewBinlog("invalid!@#$base64")

	_, err := builder.buildBinlog(ctx, binlogNode, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "BinlogReplicaController")
}

func TestBuildBinlog_NoBinlogReplicaController(t *testing.T) {
	builder := &BaseBuilder{}
	ctx := sql.NewEmptyContext()

	// Create some valid base64 data
	eventData := make([]byte, 10)
	encoded := base64.StdEncoding.EncodeToString(eventData)

	binlogNode := plan.NewBinlog(encoded)
	// Don't set controller - should get error

	_, err := builder.buildBinlog(ctx, binlogNode, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "BinlogReplicaController")
}

// mockBinlogReplicaController is a test implementation of BinlogReplicaController
type mockBinlogReplicaController struct {
	consumedEvents []mysql.BinlogEvent
	returnError    error
	hasFormatDesc  bool
}

func (m *mockBinlogReplicaController) ConsumeBinlogEvent(ctx *sql.Context, event mysql.BinlogEvent) error {
	m.consumedEvents = append(m.consumedEvents, event)
	if event.IsFormatDescription() {
		m.hasFormatDesc = true
	}
	return m.returnError
}

func (m *mockBinlogReplicaController) HasFormatDescription() bool {
	return m.hasFormatDesc
}

func (m *mockBinlogReplicaController) StartReplica(ctx *sql.Context) error {
	return nil
}

func (m *mockBinlogReplicaController) StopReplica(ctx *sql.Context) error {
	return nil
}

func (m *mockBinlogReplicaController) SetReplicationSourceOptions(ctx *sql.Context, options []binlogreplication.ReplicationOption) error {
	return nil
}

func (m *mockBinlogReplicaController) SetReplicationFilterOptions(ctx *sql.Context, options []binlogreplication.ReplicationOption) error {
	return nil
}

func (m *mockBinlogReplicaController) GetReplicaStatus(ctx *sql.Context) (*binlogreplication.ReplicaStatus, error) {
	return nil, nil
}

func (m *mockBinlogReplicaController) ResetReplica(ctx *sql.Context, resetAll bool) error {
	return nil
}

func TestBuildBinlog_WithBinlogReplicaController(t *testing.T) {
	builder := &BaseBuilder{}
	ctx := sql.NewEmptyContext()

	mockController := &mockBinlogReplicaController{}

	// Create a minimal valid binlog event (FORMAT_DESCRIPTION_EVENT)
	// Event header: timestamp(4) + type(1) + server_id(4) + event_length(4) + next_position(4) + flags(2)
	eventData := make([]byte, 19)
	eventData[4] = 0x0f
	binary.LittleEndian.PutUint32(eventData[9:13], 19) // event length

	encoded := base64.StdEncoding.EncodeToString(eventData)

	binlogNode := plan.NewBinlog(encoded).WithBinlogReplicaController(mockController).(*plan.Binlog)

	iter, err := builder.buildBinlog(ctx, binlogNode, nil)
	require.NoError(t, err)
	require.NotNil(t, iter)

	row, err := iter.Next(ctx)
	require.NoError(t, err)
	require.NotNil(t, row)
	require.Equal(t, types.OkResult{}, row[0])

	// Verify controller received one event
	require.Len(t, mockController.consumedEvents, 1)

	// Next call should return EOF
	_, err = iter.Next(ctx)
	require.Equal(t, io.EOF, err)
}

func TestBuildBinlog_MultilineBase64WithController(t *testing.T) {
	builder := &BaseBuilder{}
	ctx := sql.NewEmptyContext()

	mockController := &mockBinlogReplicaController{}

	// Create two minimal events
	event1 := make([]byte, 19)
	event1[4] = 0x0f // FORMAT_DESCRIPTION_EVENT
	binary.LittleEndian.PutUint32(event1[9:13], 19)

	event2 := make([]byte, 19)
	event2[4] = 0x02 // QUERY_EVENT
	binary.LittleEndian.PutUint32(event2[9:13], 19)

	combined := append(event1, event2...)
	part1 := base64.StdEncoding.EncodeToString(combined[:10])
	part2 := base64.StdEncoding.EncodeToString(combined[10:])
	multiline := part1 + "\n" + part2

	binlogNode := plan.NewBinlog(multiline).WithBinlogReplicaController(mockController).(*plan.Binlog)

	iter, err := builder.buildBinlog(ctx, binlogNode, nil)
	require.NoError(t, err)

	// Next() processes all events and returns single OkResult
	row, err := iter.Next(ctx)
	require.NoError(t, err)
	require.NotNil(t, row)
	require.Equal(t, types.OkResult{}, row[0])

	require.Len(t, mockController.consumedEvents, 2)

	_, err = iter.Next(ctx)
	require.Equal(t, io.EOF, err)
}

func TestBuildBinlog_ControllerError(t *testing.T) {
	builder := &BaseBuilder{}
	ctx := sql.NewEmptyContext()

	mockController := &mockBinlogReplicaController{
		returnError: sql.ErrUnsupportedFeature.New("test error"),
	}

	eventData := make([]byte, 19)
	eventData[4] = 0x0f // FORMAT_DESCRIPTION_EVENT
	binary.LittleEndian.PutUint32(eventData[9:13], 19)
	encoded := base64.StdEncoding.EncodeToString(eventData)

	binlogNode := plan.NewBinlog(encoded).WithBinlogReplicaController(mockController).(*plan.Binlog)

	iter, err := builder.buildBinlog(ctx, binlogNode, nil)
	require.NoError(t, err)

	_, err = iter.Next(ctx)
	require.Error(t, err)
	require.Contains(t, err.Error(), "test error")
}
