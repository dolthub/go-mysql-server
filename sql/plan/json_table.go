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

	"github.com/dolthub/vitess/go/vt/sqlparser"
	"github.com/oliveagle/jsonpath"

	"github.com/dolthub/go-mysql-server/sql"
)

// TODO: take a schema instead of a TableSpec?
func NewJSONTable(data []byte, path string, spec *sqlparser.TableSpec) (sql.Node, error) {
	//jsonpath.Compile()

	var json_data interface{}
	if err := json.Unmarshal(data, &json_data); err != nil {
		return nil, err
	}

	res, err := jsonpath.JsonPathLookup(json_data, path)
	if err != nil {
		return nil, err
	}

	if res != nil {
	}

	return nil, nil
}
