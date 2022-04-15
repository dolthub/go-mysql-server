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
	"github.com/dolthub/go-mysql-server/sql/transform"
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

func (r RowUpdateAccumulator) Child() sql.Node {
	return r.UnaryNode.Child
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

// CheckPrivileges implements the interface sql.Node.
func (r RowUpdateAccumulator) CheckPrivileges(ctx *sql.Context, opChecker sql.PrivilegedOperationChecker) bool {
	return r.Child().CheckPrivileges(ctx, opChecker)
}

func (r RowUpdateAccumulator) String() string {
	return r.Child().String()
}

func (r RowUpdateAccumulator) DebugString() string {
	pr := sql.NewTreePrinter()
	_ = pr.WriteNode("RowUpdateAccumulator")
	_ = pr.WriteChildren(sql.DebugString(r.Child()))
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
	rowsMatched               int
	rowsAffected              int
	schema                    sql.Schema
	clientFoundRowsCapability bool
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
	affected := u.rowsAffected
	if u.clientFoundRowsCapability {
		affected = u.rowsMatched
	}
	return sql.OkResult{
		RowsAffected: uint64(affected),
		Info: UpdateInfo{
			Matched:  u.rowsMatched,
			Updated:  u.rowsAffected,
			Warnings: 0,
		},
	}
}

func (u *updateRowHandler) RowsMatched() int64 {
	return int64(u.rowsMatched)
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

func (u *updateJoinRowHandler) RowsMatched() int64 {
	return int64(u.rowsMatched)
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
	iter             sql.RowIter
	once             sync.Once
	updateRowHandler accumulatorRowHandler
}

func (a *accumulatorIter) Next(ctx *sql.Context) (r sql.Row, err error) {
	run := false
	a.once.Do(func() {
		run = true
	})

	if !run {
		return nil, io.EOF
	}

	oldLastInsertId := ctx.Session.GetLastQueryInfo(sql.LastInsertId)
	if oldLastInsertId != 0 {
		ctx.Session.SetLastQueryInfo(sql.LastInsertId, -1)
	}

	// We close our child iterator before returning any results. In
	// particular, the LOAD DATA source iterator needs to be closed before
	// results are returned.
	defer func() {
		cerr := a.iter.Close(ctx)
		if err == nil {
			err = cerr
		}
	}()

	for {
		row, err := a.iter.Next(ctx)
		_, isIg := err.(sql.ErrInsertIgnore)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}
		if err == io.EOF {
			res := a.updateRowHandler.okResult()

			// TODO: The information flow here is pretty gnarly. We
			// set some session variables based on the result, and
			// we actually use a session variable to set
			// InsertID. This should be improved.

			// By definition, ROW_COUNT() is equal to RowsAffected.
			ctx.SetLastQueryInfo(sql.RowCount, int64(res.RowsAffected))

			// UPDATE statements also set FoundRows to the number of rows that
			// matched the WHERE clause, same as a SELECT.
			if ma, ok := a.updateRowHandler.(matchingAccumulator); ok {
				ctx.SetLastQueryInfo(sql.FoundRows, ma.RowsMatched())
			}

			newLastInsertId := ctx.Session.GetLastQueryInfo(sql.LastInsertId)
			if newLastInsertId != -1 {
				res.InsertID = uint64(newLastInsertId)
			} else {
				ctx.Session.SetLastQueryInfo(sql.LastInsertId, oldLastInsertId)
			}

			return sql.NewRow(res), nil
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

type matchingAccumulator interface {
	RowsMatched() int64
}

func (r RowUpdateAccumulator) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	rowIter, err := r.Child().RowIter(ctx, row)
	if err != nil {
		return nil, err
	}

	clientFoundRowsToggled := (ctx.Client().Capabilities & mysql.CapabilityClientFoundRows) == mysql.CapabilityClientFoundRows

	var rowHandler accumulatorRowHandler
	switch r.RowUpdateType {
	case UpdateTypeInsert:
		rowHandler = &insertRowHandler{}
	case UpdateTypeReplace:
		rowHandler = &replaceRowHandler{}
	case UpdateTypeDuplicateKeyUpdate:
		rowHandler = &onDuplicateUpdateHandler{schema: r.Child().Schema(), clientFoundRowsCapability: clientFoundRowsToggled}
	case UpdateTypeUpdate:
		schema := r.Child().Schema()
		// the schema of the update node is a self-concatenation of the underlying table's, so split it in half for new /
		// old row comparison purposes
		rowHandler = &updateRowHandler{schema: schema[:len(schema)/2], clientFoundRowsCapability: clientFoundRowsToggled}
	case UpdateTypeDelete:
		rowHandler = &deleteRowHandler{}
	case UpdateTypeJoinUpdate:
		var schema sql.Schema
		var updaterMap map[string]sql.RowUpdater
		transform.Inspect(r.Child(), func(node sql.Node) bool {
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
		iter:             rowIter,
		updateRowHandler: rowHandler,
	}, nil
}
