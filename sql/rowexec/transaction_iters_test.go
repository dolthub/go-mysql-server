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
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
)

// failingIter is a RowIter whose Next returns the error given.
type failingIter struct {
	err error
}

func (i failingIter) Next(*sql.Context) (sql.Row, error) { return nil, i.err }
func (i failingIter) Close(*sql.Context) error           { return nil }

// testTransaction is a transaction of a recordingTxSession.
type testTransaction struct{}

func (testTransaction) String() string   { return "testTransaction" }
func (testTransaction) IsReadOnly() bool { return false }

// recordingTxSession is a TransactionSession that counts how its transactions end.
type recordingTxSession struct {
	sql.Session
	commits   int
	rollbacks int
}

var _ sql.TransactionSession = (*recordingTxSession)(nil)

func (s *recordingTxSession) StartTransaction(*sql.Context, sql.TransactionCharacteristic) (sql.Transaction, error) {
	return testTransaction{}, nil
}

func (s *recordingTxSession) CommitTransaction(*sql.Context, sql.Transaction) error {
	s.commits++
	return nil
}

func (s *recordingTxSession) Rollback(*sql.Context, sql.Transaction) error {
	s.rollbacks++
	return nil
}

func (s *recordingTxSession) CreateSavepoint(*sql.Context, sql.Transaction, string) error {
	return nil
}

func (s *recordingTxSession) RollbackToSavepoint(*sql.Context, sql.Transaction, string) error {
	return nil
}

func (s *recordingTxSession) ReleaseSavepoint(*sql.Context, sql.Transaction, string) error {
	return nil
}

func TestTransactionCommittingIterFailedStatement(t *testing.T) {
	tests := []struct {
		name string
		err  error
		// explicit is whether the statement runs in a transaction that the client began explicitly.
		explicit bool
		// lost is whether the transaction is expected to be rolled back and cleared.
		lost bool
	}{
		{name: "ordinary error in explicit transaction", err: fmt.Errorf("boom"), explicit: true},
		{name: "ordinary error in autocommit transaction", err: fmt.Errorf("boom"), lost: true},
		{name: "lock wait timeout in explicit transaction", err: sql.ErrRowLockWaitTimeout.New(), explicit: true},
		{name: "lock not available in explicit transaction", err: sql.ErrRowLockNotAvailable.New("t"), explicit: true},
		{name: "serialization failure in explicit transaction", err: sql.ErrLockDeadlock.New("conflict"), explicit: true, lost: true},
		{name: "row lock deadlock in explicit transaction", err: sql.ErrRowLockDeadlock.New(), explicit: true, lost: true},
		{
			name:     "serialization failure of an insert in explicit transaction",
			err:      sql.NewWrappedInsertError(sql.Row{1}, sql.ErrLockDeadlock.New("conflict")),
			explicit: true,
			lost:     true,
		},
		{name: "row lock deadlock in autocommit transaction", err: sql.ErrRowLockDeadlock.New(), lost: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			session := &recordingTxSession{Session: sql.NewBaseSession()}
			ctx := sql.NewContext(context.Background(), sql.WithSession(session))
			ctx.SetTransaction(testTransaction{})
			ctx.SetIgnoreAutoCommit(test.explicit)

			iter := &TransactionCommittingIter{childIter: failingIter{err: test.err}, autoCommit: true}
			_, err := iter.Next(ctx)
			require.Equal(t, test.err, err)
			require.NoError(t, iter.Close(ctx))

			require.Zero(t, session.commits)
			if test.lost {
				require.Equal(t, 1, session.rollbacks)
				require.Nil(t, ctx.GetTransaction())
				require.False(t, ctx.GetIgnoreAutoCommit())
			} else {
				require.Zero(t, session.rollbacks)
				require.NotNil(t, ctx.GetTransaction())
				require.True(t, ctx.GetIgnoreAutoCommit())
			}
		})
	}
}
