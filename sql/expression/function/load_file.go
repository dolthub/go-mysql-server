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

package function

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/dolthub/go-mysql-server/sql"
)

type LoadFile struct {
	fileName sql.Expression
}

var _ sql.FunctionExpression = (*LoadFile)(nil)

func NewLoadFile(ctx *sql.Context, fileName sql.Expression) sql.Expression {
	return &LoadFile{
		fileName: fileName,
	}
}

func (l LoadFile) Resolved() bool {
	return true
}

func (l LoadFile) String() string {
	return fmt.Sprintf("LOAD_FILE(%s)", l.fileName)
}

func (l LoadFile) Type() sql.Type {
	return sql.LongBlob // TODO: Get this right
}

func (l LoadFile) IsNullable() bool {
	return false
}

// TODO: Allow FILE privileges for GRANT
func (l LoadFile) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	dir, err := getSecureFilePriv(ctx)
	if err != nil {
		return nil, err
	}

	// Read the file: Ensure it fits the max byte size
	// According to the mysql spec we must return NULL if the file is too big.
	file, err := l.getFile(ctx, row, dir)

	if err != nil {
		return nil, handleFileErrors(err)
	}
	if file == nil {
		return nil, nil
	}

	defer file.Close()

	size, isTooBig, err := isFileTooBig(ctx, file)
	if err != nil {
		return nil, err
	}
	if isTooBig {
		return nil, nil
	}

	// Finally, read the file
	data := make([]byte, size)
	_, err = file.Read(data)
	if err != nil {
		return nil, err
	}

	return data, nil
}

func (l *LoadFile) getFile(ctx *sql.Context, row sql.Row, secureFileDir string) (*os.File, error) {
	fileName, err := l.fileName.Eval(ctx, row)
	if err != nil {
		return nil, err
	}

	// If the secure_file_priv directory is not set, just read the file from whatever directory it is in
	// Otherwise determine whether the file is in the secure_file_priv directory.
	if secureFileDir == "" {
		return os.Open(fileName.(string))
	}

	// Open the two directories (secure_file_priv and the file dir) and validate they are the same.
	sDir, err := os.Open(secureFileDir)
	if err != nil {
		return nil, err
	}

	sStat, err := sDir.Stat()
	if err != nil {
		return nil, err
	}

	ffDir, err := os.Open(filepath.Dir(fileName.(string)))
	if err != nil {
		return nil, err
	}

	fStat, err := ffDir.Stat()
	if err != nil {
		return nil, err
	}

	// If the two directories are not equivalent we return nil
	if !os.SameFile(sStat, fStat) {
		return nil, nil
	}

	return os.Open(fileName.(string))
}

func getSecureFilePriv(ctx *sql.Context) (string, error) {
	val, err := ctx.Session.GetSessionVariable(ctx, "secure_file_priv")
	if err != nil {
		return "", err
	}

	return val.(string), nil
}

func isFileTooBig(ctx *sql.Context, file *os.File) (int64, bool, error) {
	fi, err := file.Stat()
	if err != nil {
		return -1, false, err
	}

	maxByteSize, err := getMaxByteSize(ctx)
	if err != nil {
		return -1, false, err
	}

	return fi.Size(), fi.Size() > maxByteSize, nil
}

func getMaxByteSize(ctx *sql.Context) (int64, error) {
	val, err := ctx.Session.GetSessionVariable(ctx, "max_allowed_packet")

	if err != nil {
		return 0, err
	}

	return val.(int64), nil
}

func handleFileErrors(err error) error {
	// If the doesn't exist we swallow that error
	if os.IsNotExist(err) {
		return nil
	}

	return err
}

func (l LoadFile) Children() []sql.Expression {
	return []sql.Expression{l.fileName}
}

func (l LoadFile) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(l, len(children), 1)
	}

	return NewLoadFile(ctx, children[0]), nil
}

func (l LoadFile) FunctionName() string {
	return "load_file"
}
