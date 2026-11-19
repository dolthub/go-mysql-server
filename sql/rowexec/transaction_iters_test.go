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

package rowexec

import (
	"context"
	"errors"
	"io"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
)

// commitCountingSession is a TransactionSession that counts commits and rollbacks.
type commitCountingSession struct {
	*sql.BaseSession
	commits, rollbacks int
}

var _ sql.TransactionSession = (*commitCountingSession)(nil)

func (s *commitCountingSession) StartTransaction(*sql.Context, sql.TransactionCharacteristic) (sql.Transaction, error) {
	return testTransaction{}, nil
}
func (s *commitCountingSession) CommitTransaction(*sql.Context, sql.Transaction) error {
	s.commits++
	return nil
}
func (s *commitCountingSession) Rollback(*sql.Context, sql.Transaction) error {
	s.rollbacks++
	return nil
}
func (s *commitCountingSession) CreateSavepoint(*sql.Context, sql.Transaction, string) error {
	return nil
}
func (s *commitCountingSession) RollbackToSavepoint(*sql.Context, sql.Transaction, string) error {
	return nil
}
func (s *commitCountingSession) ReleaseSavepoint(*sql.Context, sql.Transaction, string) error {
	return nil
}

type testTransaction struct{}

func (testTransaction) String() string   { return "test" }
func (testTransaction) IsReadOnly() bool { return false }

// failingIter returns no rows, failing with |nextErr| and |closeErr|.
type failingIter struct {
	nextErr, closeErr error
}

func (i failingIter) Next(*sql.Context) (sql.Row, error) {
	if i.nextErr != nil {
		return nil, i.nextErr
	}
	return nil, io.EOF
}
func (i failingIter) Close(*sql.Context) error { return i.closeErr }

// TestTransactionCommittingIterFailedStatement checks that an autocommit statement commits its transaction only if it
// succeeds. One that fails, while running or while closing, rolls its transaction back. In a caller's transaction, a
// failed statement leaves the transaction open, and only a DDL statement that succeeds commits it.
func TestTransactionCommittingIterFailedStatement(t *testing.T) {
	failure := errors.New("failure")
	for _, tt := range []struct {
		name               string
		ddl                bool
		explicitTx         bool
		autocommitOff      bool
		nextErr, closeErr  error
		commits, rollbacks int
		keepsTx            bool
	}{
		{name: "succeeds", commits: 1},
		{name: "fails while running", nextErr: failure, rollbacks: 1},
		{name: "fails while closing", closeErr: failure, rollbacks: 1},
		{name: "DDL succeeds", ddl: true, commits: 1},
		{name: "DDL fails while running", ddl: true, nextErr: failure, rollbacks: 1},
		{name: "DDL fails while closing", ddl: true, closeErr: failure, rollbacks: 1},
		{name: "succeeds in explicit transaction", explicitTx: true, keepsTx: true},
		{name: "fails in explicit transaction", explicitTx: true, nextErr: failure, keepsTx: true},
		{name: "DDL succeeds in explicit transaction", ddl: true, explicitTx: true, commits: 1},
		{name: "DDL fails in explicit transaction", ddl: true, explicitTx: true, nextErr: failure, keepsTx: true},
		{name: "DDL succeeds with autocommit off", ddl: true, autocommitOff: true, commits: 1},
		{name: "DDL fails with autocommit off", ddl: true, autocommitOff: true, nextErr: failure, keepsTx: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			sess := &commitCountingSession{BaseSession: sql.NewBaseSession()}
			ctx := sql.NewContext(context.Background(), sql.WithSession(sess))
			ctx.SetTransaction(testTransaction{})
			ctx.SetIgnoreAutoCommit(tt.explicitTx)
			iter := &TransactionCommittingIter{
				childIter:      failingIter{tt.nextErr, tt.closeErr},
				autoCommit:     !tt.autocommitOff,
				implicitCommit: tt.ddl,
			}

			_, err := iter.Next(ctx)
			if tt.nextErr != nil {
				require.ErrorIs(t, err, tt.nextErr)
			} else {
				require.Equal(t, io.EOF, err)
			}
			err = iter.Close(ctx)
			if tt.closeErr != nil {
				require.ErrorIs(t, err, tt.closeErr)
			} else {
				require.NoError(t, err)
			}
			if tt.keepsTx {
				require.NotNil(t, ctx.GetTransaction())
			} else {
				require.Nil(t, ctx.GetTransaction())
			}
			require.Equal(t, tt.commits, sess.commits)
			require.Equal(t, tt.rollbacks, sess.rollbacks)
		})
	}
}

// TestTransactionCommittingIterLostTransaction checks that a statement that fails with a deadlock or a serialization
// failure loses its whole transaction, even one that the client began explicitly, and that a statement that fails any
// other way leaves an explicit transaction open.
func TestTransactionCommittingIterLostTransaction(t *testing.T) {
	for _, tt := range []struct {
		name string
		err  error
		// explicit is whether the statement runs in a transaction that the client began explicitly.
		explicit bool
		// lost is whether the transaction is expected to be rolled back and cleared.
		lost bool
	}{
		{name: "ordinary error in explicit transaction", err: errors.New("failure"), explicit: true},
		{name: "lock wait timeout in explicit transaction", err: sql.ErrRowLockWaitTimeout.New(), explicit: true},
		{name: "lock not available in explicit transaction", err: sql.ErrRowLockNotAvailable.New("t"), explicit: true},
		{name: "serialization failure in explicit transaction", err: sql.ErrLockDeadlock.New("conflict"), explicit: true, lost: true},
		{
			name:     "serialization failure of an insert in explicit transaction",
			err:      sql.NewWrappedInsertError(sql.Row{1}, sql.ErrLockDeadlock.New("conflict")),
			explicit: true,
			lost:     true,
		},
		{name: "row lock deadlock in explicit transaction", err: sql.ErrRowLockDeadlock.New(), explicit: true, lost: true},
		{name: "row lock deadlock in autocommit transaction", err: sql.ErrRowLockDeadlock.New(), lost: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			sess := &commitCountingSession{BaseSession: sql.NewBaseSession()}
			ctx := sql.NewContext(context.Background(), sql.WithSession(sess))
			ctx.SetTransaction(testTransaction{})
			ctx.SetIgnoreAutoCommit(tt.explicit)

			iter := &TransactionCommittingIter{childIter: failingIter{nextErr: tt.err}, autoCommit: true}
			_, err := iter.Next(ctx)
			require.Equal(t, tt.err, err)
			require.NoError(t, iter.Close(ctx))

			require.Zero(t, sess.commits)
			if tt.lost {
				require.Equal(t, 1, sess.rollbacks)
				require.Nil(t, ctx.GetTransaction())
				require.False(t, ctx.GetIgnoreAutoCommit())
			} else {
				require.Zero(t, sess.rollbacks)
				require.NotNil(t, ctx.GetTransaction())
				require.True(t, ctx.GetIgnoreAutoCommit())
			}
		})
	}
}
