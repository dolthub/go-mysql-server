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
	"bufio"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"os"
)

type LoadData struct {
	Local bool
	File string
	Destination sql.Node
	ColumnNames []string
}

func (l LoadData) Resolved() bool {
	return l.Destination.Resolved()
}

func (l LoadData) String() string {
	return "Load data yooyoyoy"
}

func (l LoadData) Schema() sql.Schema {
	return l.Destination.Schema()
}

func (l LoadData) Children() []sql.Node {
	return []sql.Node{l.Destination}
}

func (l LoadData) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	// Get Files as an InsertRows
	file, err := os.Open("/Users/vinairachakonda/Desktop/x.txt")
	if err != nil {
		return nil, err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	var values [][]sql.Expression
	for scanner.Scan() {
		//split := strings.Split(scanner.Text(), ",")
		exprs := make([]sql.Expression, 1)
		exprs[0] = expression.NewLiteral(0, sql.Int8)
		values = append(values, exprs)
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	newValue := NewValues(values)

	return newInsertIter(ctx, l.Destination, newValue, false, nil, row)
}

func (l LoadData) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(l, len(children), 1)
	}

	l.Destination = children[0]
	return l, nil
}

func NewLoadData(local bool, file string, destination sql.Node, cols []string) *LoadData {
	return &LoadData{
		Local: local,
		File: file,
		Destination: destination,
		ColumnNames: cols,
	}
}