// Copyright 2022 Dolthub, Inc.
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
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/vitess/go/sqltypes"
)

// TODO: Consider an embeddable type
type ShowReplicaStatus struct {
	replicaController BinlogReplicaController
}

var _ sql.Node = (*ShowReplicaStatus)(nil)
var _ BinlogReplicaControllerCommand = (*ShowReplicaStatus)(nil)

func NewShowReplicaStatus() *ShowReplicaStatus {
	return &ShowReplicaStatus{}
}

func (s *ShowReplicaStatus) WithBinlogReplicaController(controller BinlogReplicaController) {
	s.replicaController = controller
}

func (s *ShowReplicaStatus) Resolved() bool {
	return true
}

func (s *ShowReplicaStatus) String() string {
	return "SHOW REPLICA STATUS"
}

func (s *ShowReplicaStatus) Schema() sql.Schema {
	return sql.Schema{
		{Name: "Source_Host", Type: sql.MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: false},
		{Name: "Source_User", Type: sql.MustCreateStringWithDefaults(sqltypes.VarChar, 2048), Default: nil, Nullable: false},
		{Name: "Source_Port", Type: sql.MustCreateStringWithDefaults(sqltypes.VarChar, 2048), Default: nil, Nullable: false},
		{Name: "Replica_IO_Running", Type: sql.MustCreateStringWithDefaults(sqltypes.VarChar, 2048), Default: nil, Nullable: false},
		{Name: "Replica_SQL_Running", Type: sql.MustCreateStringWithDefaults(sqltypes.VarChar, 2048), Default: nil, Nullable: false},
		{Name: "Last_Errno", Type: sql.MustCreateStringWithDefaults(sqltypes.VarChar, 2048), Default: nil, Nullable: false},
		{Name: "Last_Error", Type: sql.MustCreateStringWithDefaults(sqltypes.VarChar, 2048), Default: nil, Nullable: false},
		{Name: "Last_IO_Errno", Type: sql.MustCreateStringWithDefaults(sqltypes.VarChar, 2048), Default: nil, Nullable: false},
		{Name: "Last_IO_Error", Type: sql.MustCreateStringWithDefaults(sqltypes.VarChar, 2048), Default: nil, Nullable: false},
		{Name: "Last_SQL_Errno", Type: sql.MustCreateStringWithDefaults(sqltypes.VarChar, 2048), Default: nil, Nullable: false},
		{Name: "Last_SQL_Error", Type: sql.MustCreateStringWithDefaults(sqltypes.VarChar, 2048), Default: nil, Nullable: false},
		{Name: "Source_Server_Id", Type: sql.MustCreateStringWithDefaults(sqltypes.VarChar, 2048), Default: nil, Nullable: false},
		{Name: "Source_UUID", Type: sql.MustCreateStringWithDefaults(sqltypes.VarChar, 2048), Default: nil, Nullable: false},
		{Name: "Last_IO_Error_Timestamp", Type: sql.MustCreateStringWithDefaults(sqltypes.VarChar, 2048), Default: nil, Nullable: false},
		{Name: "Last_SQL_Error_Timestamp", Type: sql.MustCreateStringWithDefaults(sqltypes.VarChar, 2048), Default: nil, Nullable: false},
		{Name: "Retrieved_Gtid_Set", Type: sql.MustCreateStringWithDefaults(sqltypes.VarChar, 2048), Default: nil, Nullable: false},
		{Name: "Executed_Gtid_Set", Type: sql.MustCreateStringWithDefaults(sqltypes.VarChar, 2048), Default: nil, Nullable: false},
		{Name: "Auto_Position", Type: sql.MustCreateStringWithDefaults(sqltypes.VarChar, 2048), Default: nil, Nullable: false},
	}
}

func (s *ShowReplicaStatus) Children() []sql.Node {
	return nil
}

func (s *ShowReplicaStatus) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	if s.replicaController == nil {
		return sql.RowsToRowIter(), nil
	}

	status, err := s.replicaController.GetReplicaStatus(ctx)
	if err != nil {
		return nil, err
	}
	if status == nil {
		return sql.RowsToRowIter(), nil
	}

	row = sql.Row{
		status.SourceHost,
		status.SourceUser,
		status.SourcePort,
		status.ReplicaIoRunning,
		status.ReplicaSqlRunning,
		status.LastSqlErrNumber,
		status.LastSqlError,
		status.LastIoErrNumber,
		status.LastIoError,
		status.LastSqlErrNumber,
		status.LastSqlError,
		status.SourceServerId,
		status.SourceServerUuid,
		status.LastSqlErrorTimestamp,
		status.LastIoErrorTimestamp,
		status.RetrievedGtidSet,
		status.ExecutedGtidSet,
		status.AutoPosition,
	}

	return sql.RowsToRowIter(row), nil
}

func (s *ShowReplicaStatus) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 0 {
		return nil, sql.ErrInvalidChildrenNumber.New(s, len(children), 0)
	}

	newNode := *s
	return &newNode, nil
}

func (s *ShowReplicaStatus) CheckPrivileges(_ *sql.Context, _ sql.PrivilegedOperationChecker) bool {
	// TODO: Implement privilege support
	return true
}
