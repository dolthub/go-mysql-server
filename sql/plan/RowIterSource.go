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

type RowIterSource struct {
	schema sql.Schema
	rowChannel chan sql.Row
}

func NewRowIterSource(schema sql.Schema, rowChannel chan sql.Row) *RowIterSource {
	return &RowIterSource{schema: schema, rowChannel: rowChannel}
}

var _ sql.Node = (*RowIterSource)(nil)

func (r *RowIterSource) Resolved() bool {
	return true
}

func (r *RowIterSource) String() string {
	return fmt.Sprintf("RowIterSource()")
}

func (r *RowIterSource) Schema() sql.Schema {
	return r.schema
}

func (r *RowIterSource) Children() []sql.Node {
	return nil
}

func (r *RowIterSource) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	return &channelRowIter{
		rowChannel: r.rowChannel,
		ctx: ctx,
	}, nil
}

func (r *RowIterSource) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 0 {
		return nil, sql.ErrInvalidChildrenNumber.New(r, len(children), 0)
	}

	return r, nil
}

type channelRowIter struct {
	rowChannel chan sql.Row
	ctx *sql.Context
}

var _ sql.RowIter = (*channelRowIter)(nil)

func (c *channelRowIter) Next() (sql.Row, error) {
	select {
	case r, ok := <- c.rowChannel:
		if !ok {
			return nil, io.EOF
		}
		return r, nil
	case <-c.ctx.Done():
		return nil, io.EOF
	}
}

func (c *channelRowIter) Close(context *sql.Context) error {
	return nil
}