package stats

import (
	"io"
	"math"
	"math/rand"

	"github.com/dolthub/go-mysql-server/sql"
)

func NewNormDistIter(colCnt, rowCnt int, mean, std float64) sql.RowIter {
	return &normDistIter{cols: colCnt, cnt: rowCnt, std: std, mean: mean}
}

func NewExpDistIter(colCnt, rowCnt int, lambda float64) sql.RowIter {
	return &expDistIter{cols: colCnt, cnt: rowCnt, lambda: lambda}
}

type normDistIter struct {
	i         int
	cols      int
	cnt       int
	std, mean float64
}

var _ sql.RowIter = (*normDistIter)(nil)

func (d *normDistIter) Next(*sql.Context) (sql.Row, error) {
	if d.i > d.cnt {
		return nil, io.EOF
	}
	d.i++
	var ret sql.Row
	for i := 0; i < d.cols; i++ {
		ret = append(ret, rand.NormFloat64()*d.std+d.mean)
	}
	return ret, nil
}

func (d *normDistIter) Close(*sql.Context) error {
	return nil
}

type expDistIter struct {
	i      int
	cols   int
	cnt    int
	lambda float64
}

var _ sql.RowIter = (*expDistIter)(nil)

func (d *expDistIter) Next(*sql.Context) (sql.Row, error) {
	if d.i > d.cnt {
		return nil, io.EOF
	}
	d.i++
	var ret sql.Row
	for i := 0; i < d.cols; i++ {
		ret = append(ret, -math.Log2(rand.NormFloat64())/d.lambda)
	}
	return ret, nil
}

func (d *expDistIter) Close(*sql.Context) error {
	return nil
}
