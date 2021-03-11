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

package sql

import "fmt"

// Engine represents a sql enginer.
type Engine string

const (
	InnoDB Engine = "InnoDB"
)

var (
	engineSupport = map[Engine]string{
		InnoDB: "DEFAULT",
	}

	engineComment = map[Engine]string{
		InnoDB: "Supports transactions, row-level locking, and foreign keys",
	}

	engineTransaction = map[Engine]string{
		InnoDB: "YES",
	}

	engineXA = map[Engine]string{
		InnoDB: "YES",
	}

	engineSavepoints = map[Engine]string{
		InnoDB: "YES",
	}
)

var EngineToMySQLVals = []Engine{
	InnoDB,
}

// Support returns the server's level of support for the storage engine,
func (e Engine) Support() string {
	support, ok := engineSupport[e]
	if !ok {
		panic(fmt.Sprintf("%v does not have a default support set", e))
	}
	return support
}

// Comment returns a brief description of the storage engine.
func (e Engine) Comment() string {
	comment, ok := engineComment[e]
	if !ok {
		panic(fmt.Sprintf("%v does not have a comment", e))
	}
	return comment
}

// Transactions returns whether the storage engine supports transactions.
func (e Engine) Transactions() string {
	transaction, ok := engineTransaction[e]
	if !ok {
		panic(fmt.Sprintf("%v does not have a tranasaction", e))
	}
	return transaction
}

// XA returns whether the storage engine supports XA transactions.
func (e Engine) XA() string {
	xa, ok := engineXA[e]
	if !ok {
		panic(fmt.Sprintf("%v does not have xa support determined", e))
	}
	return xa
}

// Savepoints returns whether the storage engine supports savepoints.
func (e Engine) Savepoints() string {
	savepoints, ok := engineSavepoints[e]
	if !ok {
		panic(fmt.Sprintf("%v does not have a default savepoints set", e))
	}
	return savepoints
}

// String returns the string representation of the Engine.
func (e Engine) String() string {
	return string(e)
}
