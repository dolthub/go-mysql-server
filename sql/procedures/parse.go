// Copyright 2025 Dolthub, Inc.
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

package procedures

import (
	ast "github.com/dolthub/vitess/go/vt/sqlparser"
)

func ConvertStmt(stmt *ast.Statement) (Block, error) {
	block := Block{}
	switch s := stmt.(type) {
	case *ast.BeginEndBlock:
		// TODO: convert this into what? operations?

	}


	return block, nil
}

// Parse parses the given CREATE FUNCTION string (which must be the entire string, not just the body) into a Block
// containing the contents of the body.
func Parse(stmt *ast.Statement) ([]InterpreterOperation, error) {
	block, err := ConvertStmt(stmt)
	if err != nil {
		return nil, err
	}

	ops := make([]InterpreterOperation, 0, len(block.Body)+len(block.Variable))
	stack := NewInterpreterStack(nil)
	if err := block.AppendOperations(&ops, &stack); err != nil {
		return nil, err
	}
	return ops, nil
}