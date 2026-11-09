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
// succeeds. One that fails, while running or while closing, rolls its transaction back.
func TestTransactionCommittingIterFailedStatement(t *testing.T) {
	failure := errors.New("failure")
	for _, tt := range []struct {
		name               string
		nextErr, closeErr  error
		commits, rollbacks int
	}{
		{name: "succeeds", commits: 1},
		{name: "fails while running", nextErr: failure, rollbacks: 1},
		{name: "fails while closing", closeErr: failure, rollbacks: 1},
	} {
		t.Run(tt.name, func(t *testing.T) {
			sess := &commitCountingSession{BaseSession: sql.NewBaseSession()}
			ctx := sql.NewContext(context.Background(), sql.WithSession(sess))
			ctx.SetTransaction(testTransaction{})
			iter := &TransactionCommittingIter{childIter: failingIter{tt.nextErr, tt.closeErr}, autoCommit: true}

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
			require.Nil(t, ctx.GetTransaction())
			require.Equal(t, tt.commits, sess.commits)
			require.Equal(t, tt.rollbacks, sess.rollbacks)
		})
	}
}
