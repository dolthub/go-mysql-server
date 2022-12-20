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

package expression

import (
	"fmt"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
)

// ProcedureReference contains the state for a single CALL statement of a stored procedure.
type ProcedureReference struct {
	idToParam   []*procedureParamReferenceValue
	idToCursor  []*procedureCursorReferenceValue
	idToHandler []*procedureHandlerReferenceValue
}
type procedureParamReferenceValue struct {
	ID         int
	Name       string
	Value      interface{}
	SqlType    sql.Type
	HasBeenSet bool
}
type procedureCursorReferenceValue struct {
	ID         int
	Name       string
	SelectStmt sql.Node
	RowIter    sql.RowIter
}
type procedureHandlerReferenceValue struct {
	ID     int
	Stmt   sql.Node
	IsExit bool
	//TODO: support more than just NOT FOUND
}

// ProcedureReferencable indicates that a sql.Node takes a *ProcedureReference returns a new copy with the reference set.
type ProcedureReferencable interface {
	WithParamReference(pRef *ProcedureReference) sql.Node
}

// InitializeVariable sets the initial value for the variable.
func (ppr *ProcedureReference) InitializeVariable(id int, name string, sqlType sql.Type, val interface{}) error {
	convertedVal, err := sqlType.Convert(val)
	if err != nil {
		return err
	}
	ppr.idToParam[id] = &procedureParamReferenceValue{
		ID:         id,
		Name:       name,
		Value:      convertedVal,
		SqlType:    sqlType,
		HasBeenSet: false,
	}
	return nil
}

// InitializeCursor sets the initial state for the cursor.
func (ppr *ProcedureReference) InitializeCursor(id int, name string, selectStmt sql.Node) {
	ppr.idToCursor[id] = &procedureCursorReferenceValue{
		ID:         id,
		Name:       name,
		SelectStmt: selectStmt,
		RowIter:    nil,
	}
}

// InitializeHandler sets the given handler's statement.
func (ppr *ProcedureReference) InitializeHandler(id int, stmt sql.Node, returnsExitError bool) {
	ppr.idToHandler[id] = &procedureHandlerReferenceValue{
		ID:     id,
		Stmt:   stmt,
		IsExit: returnsExitError,
	}
}

// GetVariableValue returns the value of the given parameter.
func (ppr *ProcedureReference) GetVariableValue(id int, name string) (interface{}, error) {
	if id >= len(ppr.idToParam) {
		return nil, fmt.Errorf("cannot find value for parameter `%s`", name)
	}
	return ppr.idToParam[id].Value, nil
}

// GetVariableType returns the type of the given parameter. Returns the NULL type if the type cannot be found.
func (ppr *ProcedureReference) GetVariableType(id int) sql.Type {
	if ppr == nil || id >= len(ppr.idToParam) {
		return sql.Null
	}
	return ppr.idToParam[id].SqlType
}

// SetVariable updates the value of the given parameter.
func (ppr *ProcedureReference) SetVariable(id int, name string, val interface{}, valType sql.Type) error {
	if id >= len(ppr.idToParam) {
		return fmt.Errorf("cannot find value for parameter `%s`", name)
	}
	paramRefVal := ppr.idToParam[id]
	//TODO: do some actual type checking using the given value's type
	val, err := paramRefVal.SqlType.Convert(val)
	if err != nil {
		return err
	}
	paramRefVal.Value = val
	paramRefVal.HasBeenSet = true
	return nil
}

// VariableHasBeenSet returns whether the parameter has had its value altered from the initial value.
func (ppr *ProcedureReference) VariableHasBeenSet(id int) bool {
	if id >= len(ppr.idToParam) {
		return false
	}
	return ppr.idToParam[id].HasBeenSet
}

// GetHandler returns a boolean indicating if the handler represents an EXIT handler, and a sql.Node containing the
// logic to execute for this handler. Returns error if an invalid ID is given.
func (ppr *ProcedureReference) GetHandler(id int) (bool, sql.Node, error) {
	if id >= len(ppr.idToHandler) {
		return false, nil, fmt.Errorf("cannot find a handler matching the ID: %d", id)
	}
	handler := ppr.idToHandler[id]
	return handler.IsExit, handler.Stmt, nil
}

// OpenCursor sets the designated cursor to open.
func (ppr *ProcedureReference) OpenCursor(ctx *sql.Context, id int, name string, row sql.Row) error {
	if id >= len(ppr.idToCursor) {
		return fmt.Errorf("cannot find cursor `%s`", name)
	}
	cursorRefVal := ppr.idToCursor[id]
	if cursorRefVal.RowIter != nil {
		return sql.ErrCursorAlreadyOpen.New(name)
	}
	var err error
	cursorRefVal.RowIter, err = cursorRefVal.SelectStmt.RowIter(ctx, row)
	return err
}

// CloseCursor closes the designated cursor.
func (ppr *ProcedureReference) CloseCursor(ctx *sql.Context, id int, name string) error {
	if id >= len(ppr.idToCursor) {
		return fmt.Errorf("cannot find cursor `%s`", name)
	}
	cursorRefVal := ppr.idToCursor[id]
	if cursorRefVal.RowIter == nil {
		return sql.ErrCursorNotOpen.New(name)
	}
	err := cursorRefVal.RowIter.Close(ctx)
	cursorRefVal.RowIter = nil
	return err
}

// FetchCursor returns the next row from the designated cursor.
func (ppr *ProcedureReference) FetchCursor(ctx *sql.Context, id int, name string) (sql.Row, sql.Schema, error) {
	if id >= len(ppr.idToCursor) {
		return nil, nil, fmt.Errorf("cannot find cursor `%s`", name)
	}
	cursorRefVal := ppr.idToCursor[id]
	if cursorRefVal.RowIter == nil {
		return nil, nil, sql.ErrCursorNotOpen.New(name)
	}
	row, err := cursorRefVal.RowIter.Next(ctx)
	return row, cursorRefVal.SelectStmt.Schema(), err
}

// CloseAllCursors closes all cursors that are still open.
func (ppr *ProcedureReference) CloseAllCursors(ctx *sql.Context) error {
	var err error
	for _, cursor := range ppr.idToCursor {
		if cursor.RowIter != nil {
			nErr := cursor.RowIter.Close(ctx)
			cursor.RowIter = nil
			if err == nil {
				err = nErr
			}
		}
	}
	return err
}

func NewProcedureReference(variableCount int, cursorCount int, handlerCount int) *ProcedureReference {
	return &ProcedureReference{
		idToParam:   make([]*procedureParamReferenceValue, variableCount),
		idToCursor:  make([]*procedureCursorReferenceValue, cursorCount),
		idToHandler: make([]*procedureHandlerReferenceValue, handlerCount),
	}
}

// ProcedureParam represents the parameter of a stored procedure or stored function.
type ProcedureParam struct {
	id         int
	name       string
	pRef       *ProcedureReference
	hasBeenSet bool
}

// NewProcedureParam creates a new ProcedureParam expression.
func NewProcedureParam(id int, name string) *ProcedureParam {
	return &ProcedureParam{id: id, name: strings.ToLower(name)}
}

// Children implements the sql.Expression interface.
func (*ProcedureParam) Children() []sql.Expression {
	return nil
}

// Resolved implements the sql.Expression interface.
func (*ProcedureParam) Resolved() bool {
	return true
}

// IsNullable implements the sql.Expression interface.
func (*ProcedureParam) IsNullable() bool {
	return false
}

// Type implements the sql.Expression interface.
func (pp *ProcedureParam) Type() sql.Type {
	return pp.pRef.GetVariableType(pp.id)
}

// Name implements the Nameable interface.
func (pp *ProcedureParam) Name() string {
	return pp.name
}

// String implements the sql.Expression interface.
func (pp *ProcedureParam) String() string {
	return pp.name
}

// Eval implements the sql.Expression interface.
func (pp *ProcedureParam) Eval(ctx *sql.Context, r sql.Row) (interface{}, error) {
	return pp.pRef.GetVariableValue(pp.id, pp.name)
}

// WithChildren implements the sql.Expression interface.
func (pp *ProcedureParam) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 0 {
		return nil, sql.ErrInvalidChildrenNumber.New(pp, len(children), 0)
	}
	return pp, nil
}

// WithParamReference returns a new *ProcedureParam containing the given *ProcedureReference.
func (pp *ProcedureParam) WithParamReference(pRef *ProcedureReference) *ProcedureParam {
	npp := *pp
	npp.pRef = pRef
	return &npp
}

// Set sets the value of this procedure parameter to the given value.
func (pp *ProcedureParam) Set(val interface{}, valType sql.Type) error {
	return pp.pRef.SetVariable(pp.id, pp.name, val, valType)
}

// UnresolvedProcedureParam represents an unresolved parameter of a stored procedure or stored function.
type UnresolvedProcedureParam struct {
	name string
}

// NewUnresolvedProcedureParam creates a new UnresolvedProcedureParam expression.
func NewUnresolvedProcedureParam(name string) *UnresolvedProcedureParam {
	return &UnresolvedProcedureParam{name: strings.ToLower(name)}
}

// Children implements the sql.Expression interface.
func (*UnresolvedProcedureParam) Children() []sql.Expression {
	return nil
}

// Resolved implements the sql.Expression interface.
func (*UnresolvedProcedureParam) Resolved() bool {
	return false
}

// IsNullable implements the sql.Expression interface.
func (*UnresolvedProcedureParam) IsNullable() bool {
	return false
}

// Type implements the sql.Expression interface.
func (*UnresolvedProcedureParam) Type() sql.Type {
	return sql.Null
}

// Name implements the Nameable interface.
func (upp *UnresolvedProcedureParam) Name() string {
	return upp.name
}

// String implements the sql.Expression interface.
func (upp *UnresolvedProcedureParam) String() string {
	return upp.name
}

// Eval implements the sql.Expression interface.
func (upp *UnresolvedProcedureParam) Eval(ctx *sql.Context, r sql.Row) (interface{}, error) {
	return nil, fmt.Errorf("attempted to use unresolved procedure param '%s'", upp.name)
}

// WithChildren implements the sql.Expression interface.
func (upp *UnresolvedProcedureParam) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 0 {
		return nil, sql.ErrInvalidChildrenNumber.New(upp, len(children), 0)
	}
	return upp, nil
}
