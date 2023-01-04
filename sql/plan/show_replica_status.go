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
	"time"

	"github.com/dolthub/vitess/go/sqltypes"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/binlogreplication"
)

// ShowReplicaStatus is the plan node for the "SHOW REPLICA STATUS" statement.
// https://dev.mysql.com/doc/refman/8.0/en/show-replica-status.html
type ShowReplicaStatus struct {
	replicaController binlogreplication.BinlogReplicaController
}

var _ sql.Node = (*ShowReplicaStatus)(nil)
var _ BinlogReplicaControllerCommand = (*ShowReplicaStatus)(nil)

func NewShowReplicaStatus() *ShowReplicaStatus {
	return &ShowReplicaStatus{}
}

func (s *ShowReplicaStatus) SetBinlogReplicaController(controller binlogreplication.BinlogReplicaController) {
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
		{Name: "Replica_IO_State", Type: sql.MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: false},
		{Name: "Source_Host", Type: sql.MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: false},
		{Name: "Source_User", Type: sql.MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: false},
		{Name: "Source_Port", Type: sql.MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: false},
		{Name: "Connect_Retry", Type: sql.MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: false},
		{Name: "Source_Log_File", Type: sql.MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: false},
		{Name: "Read_Source_Log_Pos", Type: sql.MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: false},
		{Name: "Relay_Log_File", Type: sql.MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: false},
		{Name: "Relay_Log_Pos", Type: sql.MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: false},
		{Name: "Relay_Source_Log_File", Type: sql.MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: false},
		{Name: "Replica_IO_Running", Type: sql.MustCreateStringWithDefaults(sqltypes.VarChar, 128), Default: nil, Nullable: false},
		{Name: "Replica_SQL_Running", Type: sql.MustCreateStringWithDefaults(sqltypes.VarChar, 128), Default: nil, Nullable: false},
		{Name: "Replicate_Do_DB", Type: sql.MustCreateStringWithDefaults(sqltypes.VarChar, 128), Default: nil, Nullable: false},
		{Name: "Replicate_Ignore_DB", Type: sql.MustCreateStringWithDefaults(sqltypes.VarChar, 128), Default: nil, Nullable: false},
		{Name: "Replicate_Do_Table", Type: sql.MustCreateStringWithDefaults(sqltypes.VarChar, 128), Default: nil, Nullable: false},
		{Name: "Replicate_Ignore_Table", Type: sql.MustCreateStringWithDefaults(sqltypes.VarChar, 128), Default: nil, Nullable: false},
		{Name: "Replicate_Wild_Do_Table", Type: sql.MustCreateStringWithDefaults(sqltypes.VarChar, 128), Default: nil, Nullable: false},
		{Name: "Replicate_Wild_Ignore_Table", Type: sql.MustCreateStringWithDefaults(sqltypes.VarChar, 128), Default: nil, Nullable: false},
		{Name: "Last_Errno", Type: sql.MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: false},
		{Name: "Last_Error", Type: sql.MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: false},
		{Name: "Skip_Counter", Type: sql.MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: false},
		{Name: "Exec_Source_Log_Pos", Type: sql.MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: false},
		{Name: "Relay_Log_Space", Type: sql.MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: false},
		{Name: "Until_Condition", Type: sql.MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: false},
		{Name: "Until_Log_File", Type: sql.MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: false},
		{Name: "Until_Log_Pos", Type: sql.MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: false},
		{Name: "Source_SSL_Allowed", Type: sql.MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: false},
		{Name: "Source_SSL_CA_File", Type: sql.MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: false},
		{Name: "Source_SSL_CA_Path", Type: sql.MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: false},
		{Name: "Source_SSL_Cert", Type: sql.MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: false},
		{Name: "Source_SSL_Cipher", Type: sql.MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: false},
		{Name: "Source_SSL_CRL_File", Type: sql.MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: false},
		{Name: "Source_SSL_CRL_Path", Type: sql.MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: false},
		{Name: "Source_SSL_Key", Type: sql.MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: false},
		{Name: "Source_SSL_Verify_Server_Cert", Type: sql.MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: false},
		{Name: "Seconds_Behind_Source", Type: sql.MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: false},
		{Name: "Last_IO_Errno", Type: sql.MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: false},
		{Name: "Last_IO_Error", Type: sql.MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: false},
		{Name: "Last_SQL_Errno", Type: sql.MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: false},
		{Name: "Last_SQL_Error", Type: sql.MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: false},
		{Name: "Replicate_Ignore_Server_Ids", Type: sql.MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: false},
		{Name: "Source_Server_Id", Type: sql.MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: false},
		{Name: "Source_UUID", Type: sql.MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: false},
		{Name: "Source_Info_File", Type: sql.MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: false},
		{Name: "SQL_Delay", Type: sql.MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: false},
		{Name: "SQL_Remaining_Delay", Type: sql.MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: false},
		{Name: "Replica_SQL_Running_State", Type: sql.MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: false},
		{Name: "Source_Retry_Count", Type: sql.MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: false},
		{Name: "Source_Bind", Type: sql.MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: false},
		{Name: "Last_IO_Error_Timestamp", Type: sql.MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: false},
		{Name: "Last_SQL_Error_Timestamp", Type: sql.MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: false},
		{Name: "Retrieved_Gtid_Set", Type: sql.MustCreateStringWithDefaults(sqltypes.VarChar, 128), Default: nil, Nullable: false},
		{Name: "Executed_Gtid_Set", Type: sql.MustCreateStringWithDefaults(sqltypes.VarChar, 128), Default: nil, Nullable: false},
		{Name: "Auto_Position", Type: sql.MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: false},
		{Name: "Replicate_Rewrite_DB", Type: sql.MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: false},
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
		"",                       // Replica_IO_State
		status.SourceHost,        // Source_Host
		status.SourceUser,        // Source_User
		status.SourcePort,        // Source_Port
		status.ConnectRetry,      // Connect_Retry
		"INVALID",                // Source_Log_File
		0,                        // Read_Source_Log_Pos
		nil,                      // Relay_Log_File
		nil,                      // Relay_Log_Pos
		"INVALID",                // Relay_Source_Log_File
		status.ReplicaIoRunning,  // Replica_IO_Running
		status.ReplicaSqlRunning, // Replica_SQL_Running
		nil,                      // Replicate_Do_DB
		nil,                      // Replicate_Ignore_DB
		nil,                      // Replicate_Do_Table
		nil,                      // Replicate_Ignore_Table
		nil,                      // Replicate_Wild_Do_Table
		nil,                      // Replicate_Wild_Ignore_Table
		status.LastSqlErrNumber,  // Last_Errno
		status.LastSqlError,      // Last_Error
		nil,                      // Skip_Counter
		0,                        // Exec_Source_Log_Pos
		nil,                      // Relay_Log_Space
		"None",                   // Until_Condition
		nil,                      // Until_Log_File
		nil,                      // Until_Log_Pos
		"Ignored",                // Source_SSL_Allowed
		nil,                      // Source_SSL_CA_File
		nil,                      // Source_SSL_CA_Path
		nil,                      // Source_SSL_Cert
		nil,                      // Source_SSL_Cipher
		nil,                      // Source_SSL_CRL_File
		nil,                      // Source_SSL_CRL_Path
		nil,                      // Source_SSL_Key
		nil,                      // Source_SSL_Verify_Server_Cert
		0,                        // Seconds_Behind_Source
		status.LastIoErrNumber,   // Last_IO_Errno
		status.LastIoError,       // Last_IO_Error
		status.LastSqlErrNumber,  // Last_SQL_Errno
		status.LastSqlError,      // Last_SQL_Error
		nil,                      // Replicate_Ignore_Server_Ids
		status.SourceServerId,    // Source_Server_Id
		status.SourceServerUuid,  // Source_UUID
		nil,                      // Source_Info_File
		0,                        // SQL_Delay
		0,                        // SQL_Remaining_Delay
		nil,                      // Replica_SQL_Running_State
		status.SourceRetryCount,  // Source_Retry_Count
		nil,                      // Source_Bind
		formatReplicaStatusTimestamp(status.LastIoErrorTimestamp),  // Last_IO_Error_Timestamp
		formatReplicaStatusTimestamp(status.LastSqlErrorTimestamp), // Last_SQL_Error_Timestamp
		status.RetrievedGtidSet,                                    // Retrieved_Gtid_Set
		status.ExecutedGtidSet,                                     // Executed_Gtid_Set
		status.AutoPosition,                                        // Auto_Position
		nil,                                                        // Replicate_Rewrite_DB
	}

	return sql.RowsToRowIter(row), nil
}

func formatReplicaStatusTimestamp(t *time.Time) string {
	if t == nil {
		return ""
	}

	return t.Format(time.UnixDate)
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
