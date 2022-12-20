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
	"fmt"
	"strings"
	"time"

	"github.com/dolthub/go-mysql-server/sql"
)

// TODO: Move this out to a better package
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

type BinlogReplicaControllerCommand interface {
	WithBinlogReplicaController(controller BinlogReplicaController)
}

// ChangeReplicationSource is the plan node for the "CHANGE REPLICATION SOURCE TO" statement.
// https://dev.mysql.com/doc/refman/8.0/en/change-replication-source-to.html
type ChangeReplicationSource struct {
	Options           []ReplicationOption
	replicaController BinlogReplicaController // TODO: Could type embed something that does this for all the replication types
}

var _ sql.Node = (*ChangeReplicationSource)(nil)
var _ BinlogReplicaControllerCommand = (*ChangeReplicationSource)(nil)

func NewChangeReplicationSource(options []ReplicationOption) *ChangeReplicationSource {
	return &ChangeReplicationSource{
		Options: options,
	}
}

func (c *ChangeReplicationSource) WithBinlogReplicaController(controller BinlogReplicaController) {
	c.replicaController = controller
}

func (c *ChangeReplicationSource) Resolved() bool {
	return true
}

func (c *ChangeReplicationSource) String() string {
	sb := strings.Builder{}
	sb.WriteString("CHANGE REPLICATION SOURCE TO ")
	for i, option := range c.Options {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString(option.Name)
		sb.WriteString(" = ")
		sb.WriteString(option.Value)
	}
	return sb.String()
}

func (c *ChangeReplicationSource) Schema() sql.Schema {
	return nil
}

func (c *ChangeReplicationSource) Children() []sql.Node {
	return nil
}

func (c *ChangeReplicationSource) RowIter(ctx *sql.Context, _ sql.Row) (sql.RowIter, error) {
	if c.replicaController == nil {
		return nil, fmt.Errorf("no replication controller available")
	}

	err := c.replicaController.SetReplicationOptions(ctx, c.Options)
	return sql.RowsToRowIter(), err
}

func (c *ChangeReplicationSource) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 0 {
		return nil, sql.ErrInvalidChildrenNumber.New(c, len(children), 0)
	}

	newNode := *c
	return &newNode, nil
}

func (c *ChangeReplicationSource) CheckPrivileges(_ *sql.Context, _ sql.PrivilegedOperationChecker) bool {
	// TODO: implement privilege checks
	return true
}

// StartReplica is a plan node for the "START REPLICA" statement.
// https://dev.mysql.com/doc/refman/8.0/en/start-replica.html
type StartReplica struct {
	replicaController BinlogReplicaController
}

var _ sql.Node = (*StartReplica)(nil)
var _ BinlogReplicaControllerCommand = (*StartReplica)(nil)

func NewStartReplica() *StartReplica {
	return &StartReplica{}
}

func (s *StartReplica) WithBinlogReplicaController(controller BinlogReplicaController) {
	s.replicaController = controller
}

func (s *StartReplica) Resolved() bool {
	return true
}

func (s *StartReplica) String() string {
	return "START REPLICA"
}

func (s *StartReplica) Schema() sql.Schema {
	return nil
}

func (s *StartReplica) Children() []sql.Node {
	return nil
}

func (s *StartReplica) RowIter(ctx *sql.Context, _ sql.Row) (sql.RowIter, error) {
	if s.replicaController == nil {
		return nil, fmt.Errorf("no replication controller available")
	}

	err := s.replicaController.StartReplica(ctx)
	return sql.RowsToRowIter(), err
}

func (s *StartReplica) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 0 {
		return nil, sql.ErrInvalidChildrenNumber.New(s, len(children), 0)
	}

	newNode := *s
	return &newNode, nil
}

func (s *StartReplica) CheckPrivileges(_ *sql.Context, _ sql.PrivilegedOperationChecker) bool {
	// TODO: implement privilege checks
	return true
}

// StopReplica is the plan node for the "STOP REPLICA" statement.
// https://dev.mysql.com/doc/refman/8.0/en/stop-replica.html
type StopReplica struct {
	replicaController BinlogReplicaController
}

var _ sql.Node = (*StopReplica)(nil)
var _ BinlogReplicaControllerCommand = (*StopReplica)(nil)

func NewStopReplica() *StopReplica {
	return &StopReplica{}
}

func (s *StopReplica) WithBinlogReplicaController(controller BinlogReplicaController) {
	s.replicaController = controller
}

func (s *StopReplica) Resolved() bool {
	return true
}

func (s *StopReplica) String() string {
	return "STOP REPLICA"
}

func (s *StopReplica) Schema() sql.Schema {
	return nil
}

func (s *StopReplica) Children() []sql.Node {
	return nil
}

func (s *StopReplica) RowIter(ctx *sql.Context, _ sql.Row) (sql.RowIter, error) {
	if s.replicaController == nil {
		return nil, fmt.Errorf("no replication controller available")
	}

	err := s.replicaController.StopReplica(ctx)
	return sql.RowsToRowIter(), err
}

func (s *StopReplica) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 0 {
		return nil, sql.ErrInvalidChildrenNumber.New(s, len(children), 0)
	}

	newNode := *s
	return &newNode, nil
}

func (s *StopReplica) CheckPrivileges(_ *sql.Context, _ sql.PrivilegedOperationChecker) bool {
	// TODO: implement privilege checks
	return true
}
