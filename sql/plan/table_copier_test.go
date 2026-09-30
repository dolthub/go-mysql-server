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

package plan

import (
	"errors"
	"io"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
)

// TestBuildAndCloseTableCopierDestination verifies iterator closure and close-error propagation.
func TestBuildAndCloseTableCopierDestination(t *testing.T) {
	closeErr := errors.New("close failed")
	iter := &closingRowIter{closeErr: closeErr}
	builder := tableCopierTestBuilder{iter: iter}

	err := buildAndCloseTableCopierDestination(sql.NewEmptyContext(), builder, Nothing{}, nil)

	require.ErrorIs(t, err, closeErr)
	require.True(t, iter.closed)
}

type tableCopierTestBuilder struct {
	iter sql.RowIter
}

// Build returns the iterator used to verify the create phase's lifecycle.
func (b tableCopierTestBuilder) Build(*sql.Context, sql.Node, sql.Row) (sql.RowIter, error) {
	return b.iter, nil
}

type closingRowIter struct {
	closed   bool
	closeErr error
}

// Next reports that the test iterator has no rows.
func (i *closingRowIter) Next(*sql.Context) (sql.Row, error) {
	return nil, io.EOF
}

// Close records its invocation and returns the configured error.
func (i *closingRowIter) Close(*sql.Context) error {
	i.closed = true
	return i.closeErr
}
