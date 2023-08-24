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
	"os"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
)

const (
	fakeReadCommittedEnvVar = "READ_COMMITTED_HACK"
)

var fakeReadCommitted bool

func init() {
	_, ok := os.LookupEnv(fakeReadCommittedEnvVar)
	if ok {
		fakeReadCommitted = true
	}
}

// TransactionCommittingNode implements autocommit logic. It wraps relevant queries and ensures the database commits
// the transaction.
type TransactionCommittingNode struct {
	UnaryNode
}

var _ sql.Node = (*TransactionCommittingNode)(nil)
var _ sql.CollationCoercible = (*TransactionCommittingNode)(nil)

// NewTransactionCommittingNode returns a TransactionCommittingNode.
func NewTransactionCommittingNode(child sql.Node) *TransactionCommittingNode {
	return &TransactionCommittingNode{UnaryNode: UnaryNode{Child: child}}
}

// String implements the sql.Node interface.
func (t *TransactionCommittingNode) String() string {
	return t.Child().String()
}

// String implements the sql.Node interface.
func (t *TransactionCommittingNode) DebugString() string {
	return sql.DebugString(t.Child())
}

func (t *TransactionCommittingNode) IsReadOnly() bool {
	return t.Child().IsReadOnly()
}

// WithChildren implements the sql.Node interface.
func (t *TransactionCommittingNode) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 1 {
		return nil, fmt.Errorf("ds")
	}

	t2 := *t
	t2.UnaryNode = UnaryNode{Child: children[0]}
	return &t2, nil
}

// CheckPrivileges implements the sql.Node interface.
func (t *TransactionCommittingNode) CheckPrivileges(ctx *sql.Context, opChecker sql.PrivilegedOperationChecker) bool {
	return t.Child().CheckPrivileges(ctx, opChecker)
}

// CollationCoercibility implements the interface sql.CollationCoercible.
func (*TransactionCommittingNode) CollationCoercibility(ctx *sql.Context) (collation sql.CollationID, coercibility byte) {
	return sql.Collation_binary, 7
}

// Child implements the sql.UnaryNode interface.
func (t *TransactionCommittingNode) Child() sql.Node {
	return t.UnaryNode.Child
}

// IsSessionAutocommit returns true if the current session is using implicit transaction management
// through autocommit.
func IsSessionAutocommit(ctx *sql.Context) (bool, error) {
	if ReadCommitted(ctx) {
		return true, nil
	}

	autoCommitSessionVar, err := ctx.GetSessionVariable(ctx, sql.AutoCommitSessionVar)
	if err != nil {
		return false, err
	}
	return types.ConvertToBool(autoCommitSessionVar)
}

func ReadCommitted(ctx *sql.Context) bool {
	if !fakeReadCommitted {
		return false
	}

	val, err := ctx.GetSessionVariable(ctx, "transaction_isolation")
	if err != nil {
		return false
	}

	valStr, ok := val.(string)
	if !ok {
		return false
	}

	return valStr == "READ-COMMITTED"
}
