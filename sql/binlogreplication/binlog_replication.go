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
	"time"

	"github.com/dolthub/go-mysql-server/sql"
)

// TODO: error out if sql-server is not running!

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

	// SetReplicationOptions configures the binlog replica controller with the specified options. The replica controller
	// must store this configuration. If any errors are encountered processing and storing the configuration options, an
	// error is returned.
	SetReplicationOptions(ctx *sql.Context, options []ReplicationOption) error

	// GetReplicaStatus returns the current status of the replica, or nil if no replication processes are running. If
	// any problems are encountered assembling the replica's status, an error is returned.
	GetReplicaStatus(ctx *sql.Context) (*ReplicaStatus, error)
}

// ReplicaStatus stores the status of a single binlog replica and is returned by `SHOW REPLICA STATUS`.
// https://dev.mysql.com/doc/refman/8.0/en/show-replica-status.html
type ReplicaStatus struct {
	SourceHost            string
	SourceUser            string
	SourcePort            uint
	ConnectRetry          uint
	SourceRetryCount      uint
	ReplicaIoRunning      string
	ReplicaSqlRunning     string
	LastSqlErrNumber      string // Alias for LastErrNumber
	LastSqlError          string // Alias for LastError
	LastIoErrNumber       string
	LastIoError           string
	SourceServerId        string
	SourceServerUuid      string
	LastSqlErrorTimestamp time.Time
	LastIoErrorTimestamp  time.Time
	RetrievedGtidSet      string
	ExecutedGtidSet       string
	AutoPosition          bool
}

const (
	ReplicaIoNotRunning  = "No"
	ReplicaIoConnecting  = "Connecting"
	ReplicaIoRunning     = "Yes"
	ReplicaSqlNotRunning = "No"
	ReplicaSqlRunning    = "Yes"
)

type ReplicationOption struct {
	Name  string
	Value string
}

func NewReplicationOption(name string, value string) ReplicationOption {
	return ReplicationOption{
		Name:  name,
		Value: value,
	}
}
