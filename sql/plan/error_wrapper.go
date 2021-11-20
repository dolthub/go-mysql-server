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
	"github.com/dolthub/go-mysql-server/sql"
	"io"
)

type ErrorHandler func (err error)

type ErrorHandlerNode struct {
	UnaryNode
	ErrorHandler
}

var _ sql.Node = (*ErrorHandlerNode)(nil)

func NewErrorHandlerNode(child sql.Node, errorHandler ErrorHandler) *ErrorHandlerNode {
	return &ErrorHandlerNode{UnaryNode{Child: child}, errorHandler}
}

func (e ErrorHandlerNode) String() string {
	return fmt.Sprintf("ErrorHandler(%s)", e.Child.String())
}

func (e ErrorHandlerNode) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	ri, err := e.Child.RowIter(ctx, row)
	if err != nil {
		return nil, err
	}

	return &errorHandlerIter{ri,e.ErrorHandler}, nil
}

func (e ErrorHandlerNode) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(e, len(children), 1)
	}

	return NewErrorHandlerNode(children[0], e.ErrorHandler), nil
}

type errorHandlerIter struct {
	childIter sql.RowIter
	ErrorHandler
}

var _ sql.RowIter = (*errorHandlerIter)(nil)

func (e errorHandlerIter) Next() (sql.Row, error) {
	row, err := e.childIter.Next()
	if err == io.EOF {
		return row, err
	}

	if err != nil {
		e.ErrorHandler(err)
	}

	return row, nil
}

func (e errorHandlerIter) Close(context *sql.Context) error {
	return e.childIter.Close(context)
}
