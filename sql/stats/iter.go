// Copyright 2024 Dolthub, Inc.
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

package stats

import (
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/dolthub/go-mysql-server/sql/types"

	"github.com/dolthub/go-mysql-server/sql"
)

func NewStatsIter(ctx *sql.Context, dStats ...sql.Statistic) (*statsIter, error) {
	return &statsIter{
		dStats: dStats,
	}, nil
}

// statsIter reads histogram buckets into string-compatible types.
// Values that are SQL rows should be converted with statsIter.ParseRow.
// todo: make a JSON compatible container for sql.Row w/ types so that we
// can eagerly convert to sql.Row without sacrificing string printing.
type statsIter struct {
	dStats        []sql.Statistic
	i             int
	j             int
	types         []sql.Type
	qual          sql.StatQualifier
	typesStr      string
	colsStr       string
	lowerBoundStr string
	createdAt     time.Time
}

var _ sql.RowIter = (*statsIter)(nil)

func (s *statsIter) Next(ctx *sql.Context) (sql.Row, error) {
	for {
		if s.i >= len(s.dStats) {
			return nil, io.EOF
		}
		if s.j == 0 {
			s.updateIndexMeta()
		}

		dStat := s.dStats[s.i]
		if s.j >= len(dStat.Histogram()) {
			s.i++
			s.j = 0
			continue
		}

		currentJ := s.j
		s.j++
		return s.bucketToRow(currentJ, dStat.Histogram()[currentJ])
	}
}

func (s *statsIter) updateIndexMeta() {
	dStat := s.dStats[s.i]

	typesB := strings.Builder{}
	sep := ""
	for _, t := range dStat.Types() {
		typesB.WriteString(sep + t.String())
		sep = ","
	}
	s.types = dStat.Types()
	s.typesStr = typesB.String()
	s.lowerBoundStr = StringifyKey(dStat.LowerBound(), dStat.Types())
	s.colsStr = strings.Join(dStat.Columns(), ",")
	s.qual = dStat.Qualifier()
	s.createdAt = dStat.CreatedAt()
}

// mcvCnt are the number of most common values that we track
// TODO: standardize uses of this constant
const mcvCnt = 4

func (s *statsIter) bucketToRow(i int, bucket sql.HistogramBucket) (sql.Row, error) {
	// todo calculate mcvs, mcvCountsStr
	mcvCntB := strings.Builder{}
	sep := ""
	for _, cnt := range bucket.McvCounts() {
		fmt.Fprintf(&mcvCntB, "%s%v", sep, cnt)
		sep = ","
	}

	mcvs := make([]string, mcvCnt)

	for i, mcv := range bucket.Mcvs() {
		mcvs[i] = StringifyKey(mcv, s.types)
	}

	return sql.UntypedSqlRow{
		s.qual.Db(),
		s.qual.Table(),
		s.qual.Index(),
		uint64(bucket.RowCount()),
		uint64(bucket.DistinctCount()),
		uint64(bucket.NullCount()),
		s.colsStr,
		s.typesStr,
		StringifyKey(bucket.UpperBound(), s.types),
		uint64(bucket.BoundCount()),
		s.createdAt,
		mcvs[0], mcvs[1], mcvs[2], mcvs[3],
		mcvCntB.String(),
	}, nil
}

func ParseRow(rowStr string, types []sql.Type) (sql.Row, error) {
	var row sql.Row = sql.UntypedSqlRow{}
	for i, v := range strings.Split(rowStr, ",") {
		val, _, err := types[i].Convert(v)
		if err != nil {
			return nil, err
		}
		row = row.Append(sql.NewUntypedRow(val))
	}
	return row, nil
}

func StringifyKey(r sql.Row, typs []sql.Type) string {
	b := strings.Builder{}
	sep := ""
	for i, v := range r.Values() {
		typ := typs[i]
		if _, ok := typ.(sql.StringType); ok {
			typ = types.LongText
			v, _, _ = typ.Convert(v)
		}
		if v == nil {
			v = typ.Zero()
		}
		fmt.Fprintf(&b, "%s%v", sep, v)
		sep = ","
	}
	return b.String()
}

func (s *statsIter) Close(context *sql.Context) error {
	return nil
}
