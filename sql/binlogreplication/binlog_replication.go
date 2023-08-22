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

package binlogreplication

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/dolthub/go-mysql-server/sql"
)

// BinlogReplicaController allows callers to control a binlog replica. Providers built on go-mysql-server may optionally
// implement this interface and use it when constructing a SQL engine in order to receive callbacks when replication
// statements (e.g. START REPLICA, SHOW REPLICA STATUS) are being handled.
type BinlogReplicaController interface {
	// StartReplica tells the binlog replica controller to start up replication processes for the current replication
	// configuration. An error is returned if replication was unable to be started. Note the error response only signals
	// whether there was a problem with the initial replication start up. Replication could fail after being started up
	// successfully with no error response returned.
	StartReplica(ctx *sql.Context) error

	// StopReplica tells the binlog replica controller to stop all replication processes. An error is returned if there
	// were any problems stopping replication. If no replication processes were running, no error is returned.
	StopReplica(ctx *sql.Context) error

	// SetReplicationSourceOptions configures the binlog replica controller with the specified source options. The
	// replica controller must store this configuration. If any errors are encountered processing and storing the
	// configuration options, an error is returned.
	SetReplicationSourceOptions(ctx *sql.Context, options []ReplicationOption) error

	// SetReplicationFilterOptions configures the binlog replica controller with the specified filter options. Although
	// the official MySQL implementation does *NOT* persist these options, the replica controller should persist them.
	// (MySQL requires these options to be manually set after every server restart, or to be specified as command line
	// arguments when starting the MySQL process.) If any errors are encountered processing and storing the filter
	// options, an error is returned.
	SetReplicationFilterOptions(ctx *sql.Context, options []ReplicationOption) error

	// GetReplicaStatus returns the current status of the replica, or nil if no replication processes are running. If
	// any problems are encountered assembling the replica's status, an error is returned.
	GetReplicaStatus(ctx *sql.Context) (*ReplicaStatus, error)

	// ResetReplica resets the state for the replica. When the |resetAll| parameter is false, a "soft" or minimal reset
	// is performed – replication errors are reset, but connection information and filters are NOT reset. If |resetAll|
	// is true, a "hard" reset is performed – replication filters are removed, replication source options are removed,
	// and `SHOW REPLICA STATUS` shows no results. If replication is currently running, this function should return an
	// error indicating that replication needs to be stopped before it can be reset. If any errors were encountered
	// resetting the replica state, an error is returned, otherwise nil is returned if the reset was successful.
	ResetReplica(ctx *sql.Context, resetAll bool) error
}

// ReplicaStatus stores the status of a single binlog replica and is returned by `SHOW REPLICA STATUS`.
// https://dev.mysql.com/doc/refman/8.0/en/show-replica-status.html
type ReplicaStatus struct {
	SourceHost            string
	SourceUser            string
	SourcePort            uint
	ConnectRetry          uint32
	SourceRetryCount      uint64
	ReplicaIoRunning      string
	ReplicaSqlRunning     string
	LastSqlErrNumber      uint   // Alias for LastErrNumber
	LastSqlError          string // Alias for LastError
	LastIoErrNumber       uint
	LastIoError           string
	SourceServerId        string
	SourceServerUuid      string
	LastSqlErrorTimestamp *time.Time
	LastIoErrorTimestamp  *time.Time
	RetrievedGtidSet      string
	ExecutedGtidSet       string
	AutoPosition          bool
	ReplicateDoTables     []string
	ReplicateIgnoreTables []string
}

type BinlogReplicaCatalog interface {
	IsBinlogReplicaCatalog() bool
	GetBinlogReplicaController() BinlogReplicaController
}

const (
	ReplicaIoNotRunning  = "No"
	ReplicaIoConnecting  = "Connecting"
	ReplicaIoRunning     = "Yes"
	ReplicaSqlNotRunning = "No"
	ReplicaSqlRunning    = "Yes"
)

// ReplicationOption represents a single option for replication configuration, as specified through the
// CHANGE REPLICATION SOURCE TO command: https://dev.mysql.com/doc/refman/8.0/en/change-replication-source-to.html
type ReplicationOption struct {
	Name  string
	Value ReplicationOptionValue
}

// ReplicationOptionValue defines an interface for configuration option values for binlog replication. It holds the
// values of options for configuring the replication source (i.e. "CHANGE REPLICATION SOURCE TO" options) and for
// replication filtering (i.g. "SET REPLICATION FILTER" options).
type ReplicationOptionValue interface {
	fmt.Stringer

	// GetValue returns the raw, untyped option value. This method should generally not be used; callers should instead
	// find the specific type implementing the ReplicationOptionValue interface and use its functions in order to get
	// typed values.
	GetValue() interface{}
}

// StringReplicationOptionValue is a ReplicationOptionValue implementation that holds a string value.
type StringReplicationOptionValue struct {
	Value string
}

var _ ReplicationOptionValue = (*StringReplicationOptionValue)(nil)

func (ov StringReplicationOptionValue) GetValue() interface{} {
	return ov.GetValueAsString()
}

func (ov StringReplicationOptionValue) GetValueAsString() string {
	return ov.Value
}

// String implements the Stringer interface and returns a string representation of this option value.
func (ov StringReplicationOptionValue) String() string {
	return ov.Value
}

// TableNamesReplicationOptionValue is a ReplicationOptionValue implementation that holds a list of table names for
// its value.
type TableNamesReplicationOptionValue struct {
	Value []sql.UnresolvedTable
}

var _ ReplicationOptionValue = (*TableNamesReplicationOptionValue)(nil)

func (ov TableNamesReplicationOptionValue) GetValue() interface{} {
	return ov.GetValueAsTableList()
}

func (ov TableNamesReplicationOptionValue) GetValueAsTableList() []sql.UnresolvedTable {
	return ov.Value
}

// String implements the Stringer interface and returns a string representation of this option value.
func (ov TableNamesReplicationOptionValue) String() string {
	sb := strings.Builder{}
	for i, urt := range ov.Value {
		if i > 0 {
			sb.WriteString(", ")
		}
		if urt.Database().Name() != "" {
			sb.WriteString(urt.Database().Name())
			sb.WriteString(".")
		}
		sb.WriteString(urt.Name())
	}
	return sb.String()
}

// IntegerReplicationOptionValue is a ReplicationOptionValue implementation that holds an integer value.
type IntegerReplicationOptionValue struct {
	Value int
}

var _ ReplicationOptionValue = (*IntegerReplicationOptionValue)(nil)

func (ov IntegerReplicationOptionValue) GetValue() interface{} {
	return ov.GetValueAsInt()
}

func (ov IntegerReplicationOptionValue) GetValueAsInt() int {
	return ov.Value
}

// String implements the Stringer interface and returns a string representation of this option value.
func (ov IntegerReplicationOptionValue) String() string {
	return strconv.Itoa(ov.Value)
}

// NewReplicationOption creates a new ReplicationOption instance, with the specified |name| and |value|.
func NewReplicationOption(name string, value ReplicationOptionValue) *ReplicationOption {
	return &ReplicationOption{
		Name:  name,
		Value: value,
	}
}
