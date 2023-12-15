package stats

import (
	"container/heap"
	"github.com/dolthub/go-mysql-server/sql"
)

// An SqlHeap is a min-heap of ints.
type SqlHeap struct {
	arr   []sql.Row
	types []sql.Type
	k     int
}

func NewSqlHeap(types []sql.Type, k int) *SqlHeap {
	return &SqlHeap{arr: make([]sql.Row, 0), types: types, k: k}
}

func (h SqlHeap) Len() int { return len(h.arr) }
func (h SqlHeap) Less(i, j int) bool {
	k := 0
	l := h.arr[i]
	r := h.arr[j]
	for k < len(l) {
		if cmp, _ := h.types[k].Compare(l[k], r[k]); cmp != 0 {
			break
		}
		k++
	}
	if k == len(l) {
		return true
	}
	cmp, _ := h.types[k].Compare(l[k], r[k])
	return cmp <= 0
}
func (h SqlHeap) Swap(i, j int) { h.arr[i], h.arr[j] = h.arr[j], h.arr[i] }

func (h *SqlHeap) Push(x any) {
	// Push and Pop use pointer receivers because they modify the slice's length,
	// not just its contents.
	h.arr = append(h.arr, x.(sql.Row))
	if len(h.arr) > h.k {
		heap.Pop(h)
	}
}

func (h *SqlHeap) Pop() any {
	old := h.arr
	n := len(old)
	x := old[n-1]
	h.arr = old[0 : n-1]
	return x
}
