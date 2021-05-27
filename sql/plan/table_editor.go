// Copyright 2021 Dolthub, Inc.
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
	"io"
	"sync"

	"github.com/dolthub/go-mysql-server/sql"
)

// tableEditorIter wraps the given iterator and calls the Begin and Complete functions on the given table.
type tableEditorIter struct {
	once             *sync.Once
	onceCtx          *sql.Context
	editor           sql.TableEditor
	inner            sql.RowIter
	errorEncountered error
}

var _ sql.RowIter = (*tableEditorIter)(nil)

// NewTableEditorIter returns a new *tableEditorIter by wrapping the given iterator. If the
// "statement_boundaries" session variable is set to false, then the original iterator is returned.
func NewTableEditorIter(ctx *sql.Context, table sql.TableEditor, wrappedIter sql.RowIter) sql.RowIter {
	return &tableEditorIter{
		once:             &sync.Once{},
		onceCtx:          ctx,
		editor:           table,
		inner:            wrappedIter,
		errorEncountered: nil,
	}
}

// Next implements the interface sql.RowIter.
func (s *tableEditorIter) Next() (sql.Row, error) {
	s.once.Do(func() {
		s.editor.StatementBegin(s.onceCtx)
	})
	row, err := s.inner.Next()
	if err != nil && err != io.EOF && !ErrInsertIgnore.Is(err) {
		s.errorEncountered = err
	}
	return row, err
}

// Close implements the interface sql.RowIter.
func (s *tableEditorIter) Close(ctx *sql.Context) error {
	var err error
	if s.errorEncountered != nil {
		err = s.editor.DiscardChanges(ctx, s.errorEncountered)
	} else {
		err = s.editor.StatementComplete(ctx)
	}
	if err != nil {
		_ = s.inner.Close(ctx)
	} else {
		err = s.inner.Close(ctx)
	}
	return err
}
