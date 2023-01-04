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
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/binlogreplication"
	"strings"
)

// BinlogReplicaControllerCommand represents a SQL statement that requires a BinlogReplicaController
// (e.g. Start Replica, Show Replica Status).
type BinlogReplicaControllerCommand interface {
	// SetBinlogReplicaController sets the BinlogReplicaController for this replication command to use.
	SetBinlogReplicaController(controller binlogreplication.BinlogReplicaController)
}

// ChangeReplicationSource is the plan node for the "CHANGE REPLICATION SOURCE TO" statement.
// https://dev.mysql.com/doc/refman/8.0/en/change-replication-source-to.html
type ChangeReplicationSource struct {
	Options           []binlogreplication.ReplicationOption
	replicaController binlogreplication.BinlogReplicaController
}

var _ sql.Node = (*ChangeReplicationSource)(nil)
var _ BinlogReplicaControllerCommand = (*ChangeReplicationSource)(nil)

func NewChangeReplicationSource(options []binlogreplication.ReplicationOption) *ChangeReplicationSource {
	return &ChangeReplicationSource{
		Options: options,
	}
}

func (c *ChangeReplicationSource) SetBinlogReplicaController(controller binlogreplication.BinlogReplicaController) {
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
	replicaController binlogreplication.BinlogReplicaController
}

var _ sql.Node = (*StartReplica)(nil)
var _ BinlogReplicaControllerCommand = (*StartReplica)(nil)

func NewStartReplica() *StartReplica {
	return &StartReplica{}
}

func (s *StartReplica) SetBinlogReplicaController(controller binlogreplication.BinlogReplicaController) {
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
	replicaController binlogreplication.BinlogReplicaController
}

var _ sql.Node = (*StopReplica)(nil)
var _ BinlogReplicaControllerCommand = (*StopReplica)(nil)

func NewStopReplica() *StopReplica {
	return &StopReplica{}
}

func (s *StopReplica) SetBinlogReplicaController(controller binlogreplication.BinlogReplicaController) {
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
