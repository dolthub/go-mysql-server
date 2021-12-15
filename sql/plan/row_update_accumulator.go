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

package plan

import (
	"fmt"
	"io"
	"sync"

	"github.com/dolthub/vitess/go/mysql"

	"github.com/dolthub/go-mysql-server/sql"
)

type RowUpdateType int

const (
	UpdateTypeInsert RowUpdateType = iota
	UpdateTypeReplace
	UpdateTypeDuplicateKeyUpdate
	UpdateTypeUpdate
	UpdateTypeDelete
	UpdateTypeJoinUpdate
)

// RowUpdateAccumulator wraps other nodes that update tables, and returns their results as OKResults with the appropriate
// fields set.
type RowUpdateAccumulator struct {
	UnaryNode
	RowUpdateType
}

// NewRowUpdateResult returns a new RowUpdateResult with the given node to wrap.
func NewRowUpdateAccumulator(n sql.Node, updateType RowUpdateType) *RowUpdateAccumulator {
	return &RowUpdateAccumulator{
		UnaryNode: UnaryNode{
			Child: n,
		},
		RowUpdateType: updateType,
	}
}

func (r RowUpdateAccumulator) Schema() sql.Schema {
	return sql.OkResultSchema
}

func (r RowUpdateAccumulator) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(r, 1, len(children))
	}
	return NewRowUpdateAccumulator(children[0], r.RowUpdateType), nil
}

func (r RowUpdateAccumulator) String() string {
	return r.Child.String()
}

func (r RowUpdateAccumulator) DebugString() string {
	pr := sql.NewTreePrinter()
	_ = pr.WriteNode("RowUpdateAccumulator")
	_ = pr.WriteChildren(sql.DebugString(r.Child))
	return pr.String()
}

type accumulatorRowHandler interface {
	handleRowUpdate(row sql.Row) error
	okResult() sql.OkResult
}

type insertRowHandler struct {
	rowsAffected int
}

func (i *insertRowHandler) handleRowUpdate(_ sql.Row) error {
	i.rowsAffected++
	return nil
}

func (i *insertRowHandler) okResult() sql.OkResult {
	// TODO: the auto inserted id should be in this result. Needs to be passed up by the insert iter, which is a larger
	//  change.
	return sql.NewOkResult(i.rowsAffected)
}

type replaceRowHandler struct {
	rowsAffected int
}

func (r *replaceRowHandler) handleRowUpdate(row sql.Row) error {
	r.rowsAffected++

	// If a row was deleted as well as inserted, increment the counter again. A row was deleted if at least one column in
	// the first half of the row is non-null.
	for i := 0; i < len(row)/2; i++ {
		if row[i] != nil {
			r.rowsAffected++
			break
		}
	}

	return nil
}

func (r *replaceRowHandler) okResult() sql.OkResult {
	return sql.NewOkResult(r.rowsAffected)
}

type onDuplicateUpdateHandler struct {
	rowsAffected              int
	schema                    sql.Schema
	clientFoundRowsCapability bool
}

func (o *onDuplicateUpdateHandler) handleRowUpdate(row sql.Row) error {
	// See https://dev.mysql.com/doc/refman/8.0/en/insert-on-duplicate.html for row count semantics
	// If a row was inserted, increment by 1
	if len(row) == len(o.schema) {
		o.rowsAffected++
		return nil
	}

	// Otherwise (a row was updated), increment by 2 if the row changed, 0 if not
	oldRow := row[:len(row)/2]
	newRow := row[len(row)/2:]
	if equals, err := oldRow.Equals(newRow, o.schema); err == nil {
		if equals {
			// Ig the CLIENT_FOUND_ROWS capabilities flag is set, increment by 1 if a row stays the same.
			if o.clientFoundRowsCapability {
				o.rowsAffected++
			}
		} else {
			o.rowsAffected += 2
		}
	} else {
		o.rowsAffected++
	}

	return nil
}

func (o *onDuplicateUpdateHandler) okResult() sql.OkResult {
	return sql.NewOkResult(o.rowsAffected)
}

type updateRowHandler struct {
	rowsMatched  int
	rowsAffected int
	schema       sql.Schema
}

func (u *updateRowHandler) handleRowUpdate(row sql.Row) error {
	u.rowsMatched++
	oldRow := row[:len(row)/2]
	newRow := row[len(row)/2:]
	if equals, err := oldRow.Equals(newRow, u.schema); err == nil {
		if !equals {
			u.rowsAffected++
		}
	} else {
		return err
	}
	return nil
}

func (u *updateRowHandler) okResult() sql.OkResult {
	return sql.OkResult{
		RowsAffected: uint64(u.rowsAffected),
		Info: UpdateInfo{
			Matched:  u.rowsMatched,
			Updated:  u.rowsAffected,
			Warnings: 0,
		},
	}
}

// updateJoinRowHandler handles row update count for all UPDATEs that use a JOIN.
type updateJoinRowHandler struct {
	rowsMatched  int
	rowsAffected int
	joinSchema   sql.Schema
	tableMap     map[string]sql.Schema // Needs to only be the tables that can be updated.
	updaterMap   map[string]sql.RowUpdater
}

func (u *updateJoinRowHandler) handleRowUpdate(row sql.Row) error {
	oldJoinRow := row[:len(row)/2]
	newJoinRow := row[len(row)/2:]

	tableToOldRow := splitRowIntoTableRowMap(oldJoinRow, u.joinSchema)
	tableToNewRow := splitRowIntoTableRowMap(newJoinRow, u.joinSchema)

	for tableName, _ := range u.updaterMap {
		u.rowsMatched++ // TODO: This currently returns the incorrect answer
		tableOldRow := tableToOldRow[tableName]
		tableNewRow := tableToNewRow[tableName]
		if equals, err := tableOldRow.Equals(tableNewRow, u.tableMap[tableName]); err == nil {
			if !equals {
				u.rowsAffected++
			}
		} else {
			return err
		}
	}
	return nil
}

func (u *updateJoinRowHandler) okResult() sql.OkResult {
	return sql.OkResult{
		RowsAffected: uint64(u.rowsAffected),
		Info: UpdateInfo{
			Matched:  u.rowsMatched,
			Updated:  u.rowsAffected,
			Warnings: 0,
		},
	}
}

// recreateTableSchemaFromJoinSchema takes a join schema and recreates each individual tables schema.
func recreateTableSchemaFromJoinSchema(joinSchema sql.Schema) map[string]sql.Schema {
	ret := make(map[string]sql.Schema, 0)

	for _, c := range joinSchema {
		potential, exists := ret[c.Source]
		if exists {
			ret[c.Source] = append(potential, c)
		} else {
			ret[c.Source] = sql.Schema{c}
		}
	}

	return ret
}

type deleteRowHandler struct {
	rowsAffected int
}

func (u *deleteRowHandler) handleRowUpdate(row sql.Row) error {
	u.rowsAffected++
	return nil
}

func (u *deleteRowHandler) okResult() sql.OkResult {
	return sql.NewOkResult(u.rowsAffected)
}

type accumulatorIter struct {
	ctx              *sql.Context
	iter             sql.RowIter
	once             sync.Once
	updateRowHandler accumulatorRowHandler
}

func (a *accumulatorIter) Next() (r sql.Row, err error) {
	run := false
	a.once.Do(func() {
		run = true
	})

	if !run {
		return nil, io.EOF
	}

	// We close our child iterator before returning any results. In
	// particular, the LOAD DATA source iterator needs to be closed before
	// results are returned.
	defer func() {
		cerr := a.iter.Close(a.ctx)
		if err == nil {
			err = cerr
		}

		if err == nil {
			result := a.updateRowHandler.okResult()
			a.ctx.SetLastQueryInfo(sql.RowCount, int64(result.RowsAffected))

			// For UPDATE, the affected-rows value is the number of rows “found”; that is, matched by the WHERE clause for FOUND_ROWS
			// cc. https://dev.mysql.com/doc/c-api/8.0/en/mysql-affected-rows.html
			if au, ok := a.updateRowHandler.(*updateRowHandler); ok {
				a.ctx.SetLastQueryInfo(sql.FoundRows, int64(au.rowsMatched))
			}
		}
	}()

	for {
		row, err := a.iter.Next()
		_, isIg := err.(sql.ErrInsertIgnore)

		if err == io.EOF {
			return sql.NewRow(a.updateRowHandler.okResult()), nil
		} else if isIg {
			continue
		} else if err != nil {
			return nil, err
		}

		err = a.updateRowHandler.handleRowUpdate(row)
		if err != nil {
			return nil, err
		}
	}
}

func (a *accumulatorIter) Close(ctx *sql.Context) error {
	return nil
}

func (r RowUpdateAccumulator) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	rowIter, err := r.Child.RowIter(ctx, row)
	if err != nil {
		return nil, err
	}

	var rowHandler accumulatorRowHandler
	switch r.RowUpdateType {
	case UpdateTypeInsert:
		rowHandler = &insertRowHandler{}
	case UpdateTypeReplace:
		rowHandler = &replaceRowHandler{}
	case UpdateTypeDuplicateKeyUpdate:
		clientFoundRowsToggled := (ctx.Client().Capabilities & mysql.CapabilityClientFoundRows) == mysql.CapabilityClientFoundRows
		rowHandler = &onDuplicateUpdateHandler{schema: r.Child.Schema(), clientFoundRowsCapability: clientFoundRowsToggled}
	case UpdateTypeUpdate:
		schema := r.Child.Schema()
		// the schema of the update node is a self-concatenation of the underlying table's, so split it in half for new /
		// old row comparison purposes
		rowHandler = &updateRowHandler{schema: schema[:len(schema)/2]}
	case UpdateTypeDelete:
		rowHandler = &deleteRowHandler{}
	case UpdateTypeJoinUpdate:
		var schema sql.Schema
		var updaterMap map[string]sql.RowUpdater
		Inspect(r.Child, func(node sql.Node) bool {
			switch node.(type) {
			case JoinNode, *CrossJoin, *Project, *IndexedJoin:
				schema = node.Schema()
				return false
			case *UpdateJoin:
				updaterMap = node.(*UpdateJoin).updaters
				return true
			}

			return true
		})

		if schema == nil {
			return nil, fmt.Errorf("error: No JoinNode found in query plan to go along with an UpdateTypeJoinUpdate")
		}

		rowHandler = &updateJoinRowHandler{joinSchema: schema, tableMap: recreateTableSchemaFromJoinSchema(schema), updaterMap: updaterMap}
	default:
		panic(fmt.Sprintf("Unrecognized RowUpdateType %d", r.RowUpdateType))
	}

	return &accumulatorIter{
		ctx:              ctx,
		iter:             rowIter,
		updateRowHandler: rowHandler,
	}, nil
}
