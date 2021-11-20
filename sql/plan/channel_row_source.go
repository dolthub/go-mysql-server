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

type ChannelRowSource struct {
	schema sql.Schema
	rowChannel chan sql.Row
}

func NewRowIterSource(schema sql.Schema, rowChannel chan sql.Row) *ChannelRowSource {
	return &ChannelRowSource{schema: schema, rowChannel: rowChannel}
}

var _ sql.Node = (*ChannelRowSource)(nil)

func (c *ChannelRowSource) Resolved() bool {
	return true
}

func (c *ChannelRowSource) String() string {
	return fmt.Sprintf("ChannelRowSource()")
}

func (c *ChannelRowSource) Schema() sql.Schema {
	return c.schema
}

func (c *ChannelRowSource) Children() []sql.Node {
	return nil
}

func (c *ChannelRowSource) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	return &channelRowIter{
		rowChannel: c.rowChannel,
		ctx:        ctx,
	}, nil
}

func (c *ChannelRowSource) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 0 {
		return nil, sql.ErrInvalidChildrenNumber.New(c, len(children), 0)
	}

	return c, nil
}

type channelRowIter struct {
	rowChannel chan sql.Row
	ctx *sql.Context
}

var _ sql.RowIter = (*channelRowIter)(nil)

func (c *channelRowIter) Next() (sql.Row, error) {
	for r := range c.rowChannel {
		select {
		case <-c.ctx.Done():
			return nil, io.EOF
		default:
			return r, nil
		}
	}
	return nil, io.EOF
}

func (c *channelRowIter) Close(context *sql.Context) error {
	return nil
}