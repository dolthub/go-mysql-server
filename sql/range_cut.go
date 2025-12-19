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

import (
	"context"
	"fmt"
)

type BoundType byte

const (
	Above BoundType = iota
	Below
	AboveNull
	BelowNull
	AboveAll
)

// TODO: Consider just making this one struct with Range
type Bound struct {
	Key       any
	BoundType BoundType
}

func NewBound(key any, boundType BoundType) *Bound {
	return &Bound{
		Key:       key,
		BoundType: boundType,
	}
}

func NewAboveNullBound() *Bound {
	return &Bound{BoundType: AboveNull}
}

func NewBelowNullBound() *Bound {
	return &Bound{BoundType: BelowNull}
}

func NewAboveAllBound() *Bound {
	return &Bound{BoundType: AboveAll}
}

func (b *Bound) Compare(ctx context.Context, other *Bound, typ Type) (int, error) {
	// TODO: handle extended type?
	switch b.BoundType {
	case Above:
		switch other.BoundType {
		case Above:
			return typ.Compare(ctx, b.Key, other.Key)
		case Below:
			cmp, err := typ.Compare(ctx, b.Key, other.Key)
			if err != nil {
				return 0, err
			}
			if cmp == -1 {
				return -1, nil
			}
			return 1, nil
		case BelowNull, AboveNull:
			return 1, nil
		case AboveAll:
			return -1, nil
		default:
			panic("Unknown bound type")
		}
	case Below:
		switch other.BoundType {
		case Above:
			cmp, err := typ.Compare(ctx, b.Key, other.Key)
			if err != nil {
				return 0, err
			}
			if cmp == 1 {
				return 1, nil
			}
			return -1, nil
		case Below:
			return typ.Compare(ctx, b.Key, other.Key)
		case BelowNull, AboveNull:
			return 1, nil
		case AboveAll:
			return -1, nil
		default:
			panic("Unknown bound type")
		}
	case AboveNull:
		switch other.BoundType {
		case AboveNull:
			return 0, nil
		case BelowNull:
			return 1, nil
		default:
			return -1, nil
		}
	case BelowNull:
		switch other.BoundType {
		case BelowNull:
			return 0, nil
		default:
			return -1, nil
		}
	case AboveAll:
		switch other.BoundType {
		case AboveAll:
			return 0, nil
		default:
			return 1, nil
		}
	default:
		panic("Unknown bound type")
	}
}

func (b *Bound) String() string {
	switch b.BoundType {
	case Above:
		return fmt.Sprintf("Above[%v]", b.Key)
	case Below:
		return fmt.Sprintf("Below[%v]", b.Key)
	case AboveNull:
		return "AboveNull"
	case BelowNull:
		return "BelowNull"
	case AboveAll:
		return "AboveAll"
	default:
		panic("Unknown bound type")
	}
}

func (b *Bound) IsBinding() bool {
	return b.BoundType == Above || b.BoundType == Below
}
