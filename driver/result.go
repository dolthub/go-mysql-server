// Copyright 2020-2021 Dolthub, Inc.
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

package driver

import (
	"errors"
	"io"

	"github.com/dolthub/go-mysql-server/sql"
)

// Result is the result of a query execution.
type Result struct {
	ctx    *sql.Context
	rows   sql.RowIter
	result *sql.OkResult
}

func (r *Result) ok() error {
	if r.result != nil {
		return nil
	}

	defer func() {
		r.rows.Close(r.ctx)
		r.rows = nil
	}()

	for {
		row, err := r.rows.Next()
		if errors.Is(err, io.EOF) {
			r.result = new(sql.OkResult)
			return nil
		}
		if err != nil {
			return err
		}

		if len(row) != 1 {
			continue
		}

		result, ok := row[0].(sql.OkResult)
		if ok {
			r.result = &result
			return nil
		}
	}
}

// LastInsertId returns the database's auto-generated ID
// after, for example, an INSERT into a table with primary
// key.
func (r *Result) LastInsertId() (int64, error) {
	if err := r.ok(); err != nil {
		return 0, err
	}
	return int64(r.result.InsertID), nil
}

// RowsAffected returns the number of rows affected by the
// query.
func (r *Result) RowsAffected() (int64, error) {
	if err := r.ok(); err != nil {
		return 0, err
	}
	return int64(r.result.RowsAffected), nil
}
