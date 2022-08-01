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
	"encoding/json"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/vitess/go/vt/sqlparser"
	"github.com/oliveagle/jsonpath"
)

type JSONTable struct {
	name   string
	schema sql.PrimaryKeySchema
	data   map[string]interface{}
	rowIdx uint64
}

var _ sql.Table = &JSONTable{}

// Name implements the sql.Table interface
func (t *JSONTable) Name() string {
	return t.name
}

// String implements the sql.Table interface
func (t *JSONTable) String() string {
	return t.name
}

// Schema implements the sql.Table interface
func (t *JSONTable) Schema() sql.Schema {
	return t.schema.Schema
}

// Partitions implements the sql.Table interface
func (t *JSONTable) Partitions(ctx *sql.Context) (sql.PartitionIter, error) {
	return nil, nil
}

// PartitionRows implements the sql.Table interface
func (t *JSONTable) PartitionRows(ctx *sql.Context, partition sql.Partition) (sql.RowIter, error) {
	return nil, nil
}

// TODO: maybe just use in-memory table?
func NewJSONTable(data []byte, path string, spec *sqlparser.TableSpec) (sql.Node, error) {
	var jsonData interface{}
	if err := json.Unmarshal(data, &jsonData); err != nil {
		return nil, err
	}

	// Get data specified from initial path
	jsonPathData, err := jsonpath.JsonPathLookup(jsonData, path)
	if err != nil {
		return nil, err
	}

	// TODO: use this data to somehow create a table
	// Do I create something in memory and have a RowIter?
	for _, col := range spec.Columns {
		res, err := jsonpath.JsonPathLookup(jsonPathData, col.Type.Path)
		if err != nil {
			return nil, err
		}
		if res != nil {
		}
	}

	return nil, nil
}
