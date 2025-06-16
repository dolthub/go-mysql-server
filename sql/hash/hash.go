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

package hash

import (
	"fmt"
	"sync"

	"github.com/cespare/xxhash/v2"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
)

var digestPool = sync.Pool{
	New: func() any {
		return xxhash.New()
	},
}

// HashOf returns a hash of the given value to be used as key in a cache.
func HashOf(ctx *sql.Context, sch sql.Schema, row sql.Row) (uint64, error) {
	hash := digestPool.Get().(*xxhash.Digest)
	hash.Reset()
	defer digestPool.Put(hash)
	for i, v := range row {
		if i > 0 {
			// separate each value in the row with a nil byte
			if _, err := hash.Write([]byte{0}); err != nil {
				return 0, err
			}
		}

		v, err := sql.UnwrapAny(ctx, v)
		if err != nil {
			return 0, fmt.Errorf("error unwrapping value: %w", err)
		}

		// TODO: we may not always have the type information available, so we check schema length.
		//   Then, defer to original behavior
		if i >= len(sch) || v == nil {
			_, err := fmt.Fprintf(hash, "%v", v)
			if err != nil {
				return 0, err
			}
			continue
		}

		switch typ := sch[i].Type.(type) {
		case types.ExtendedType:
			// TODO: this should use types.ExtendedType.SerializeValue, but there are some doltgres conversion issues
			//   we need to address. Resort to old behavior for now.
			_, err = fmt.Fprintf(hash, "%v", v)
			if err != nil {
				return 0, err
			}
		case types.StringType:
			var strVal string
			strVal, err = types.ConvertToString(ctx, v, typ, nil)
			if err != nil {
				return 0, err
			}
			err = typ.Collation().WriteWeightString(hash, strVal)
			if err != nil {
				return 0, err
			}
		default:
			// TODO: probably much faster to do this with a type switch
			_, err = fmt.Fprintf(hash, "%v", v)
			if err != nil {
				return 0, err
			}
		}
	}
	return hash.Sum64(), nil
}
