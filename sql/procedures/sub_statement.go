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

package procedures

import (
	ast "github.com/dolthub/vitess/go/vt/sqlparser"

	"github.com/dolthub/go-mysql-server/sql"
)

// SubStatementError returns the error for |stmt| if it may not run as part of another statement, as in a trigger or a
// procedure a trigger calls, or nil if it may. Such statements can't end the enclosing statement's transaction, so
// MySQL rejects transaction control, statements that commit implicitly, and setting autocommit. A call to an external
// procedure in |procs| that ends the transaction is rejected the same way.
func SubStatementError(ctx *sql.Context, procs sql.ExternalStoredProcedureProvider, stmt ast.Statement) error {
	switch s := stmt.(type) {
	case *ast.Call:
		if procs == nil {
			return nil
		}
		// External procedures take precedence over stored ones of the same name, as when the CALL is built.
		esp, err := procs.ExternalStoredProcedure(ctx, s.ProcName.Name.String(), len(s.Params))
		if err != nil {
			return err
		}
		if esp != nil && esp.NotAllowedInSubStatement {
			return sql.ErrCommitNotAllowedInSubStatement.New()
		}
	case *ast.Begin, *ast.Commit, *ast.Rollback,
		*ast.AlterTable, *ast.DBDDL,
		*ast.LockTables, *ast.UnlockTables,
		*ast.CreateUser, *ast.RenameUser, *ast.DropUser, *ast.CreateRole, *ast.DropRole,
		*ast.GrantPrivilege, *ast.GrantRole, *ast.GrantProxy,
		*ast.RevokePrivilege, *ast.RevokeRole, *ast.RevokeProxy,
		*ast.ChangeReplicationSource, *ast.ChangeReplicationFilter, *ast.StartReplica, *ast.StopReplica:
		return sql.ErrCommitNotAllowedInSubStatement.New()
	case *ast.DDL:
		// Temporary tables don't commit.
		if !s.Temporary {
			return sql.ErrCommitNotAllowedInSubStatement.New()
		}
	case *ast.Set:
		for _, expr := range s.Exprs {
			if expr.Scope != ast.SetScope_User && expr.Name.EqualString(sql.AutoCommitSessionVar) {
				return sql.ErrSetAutocommitInSubStatement.New()
			}
		}
	}
	return nil
}

// subStatementOpsError returns the error for the first statement in |ops| that may not run as part of another
// statement, or nil. Like MySQL, it checks every statement, including handlers and statements that may not run. The
// stored procedures that |ops| calls are checked when they are called, but calls to external procedures are checked
// here.
func subStatementOpsError(ctx *sql.Context, procs sql.ExternalStoredProcedureProvider, ops []*InterpreterOperation) error {
	for _, op := range ops {
		switch op.OpCode {
		case OpCode_Execute, OpCode_Call:
			if err := SubStatementError(ctx, procs, op.PrimaryData); err != nil {
				return err
			}
		case OpCode_Declare:
			declare, ok := op.PrimaryData.(*ast.Declare)
			if !ok || declare.Handler == nil {
				continue
			}
			handlerOps := make([]*InterpreterOperation, 0, 1)
			if err := ConvertStmt(&handlerOps, NewInterpreterStack(), declare.Handler.Statement); err != nil {
				// The handler fails the same way if it runs.
				continue
			}
			if err := subStatementOpsError(ctx, procs, handlerOps); err != nil {
				return err
			}
		}
	}
	return nil
}
