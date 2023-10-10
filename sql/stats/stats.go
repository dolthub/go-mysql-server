package stats

import (
	"fmt"
	"strings"
	"time"
)

var EmptyStats = Stats{}

type Stats struct {
	Rows      uint64    `json:"row_count"`
	Distinct  uint64    `json:"distinct_count"`
	Nulls     uint64    `json:"null_count"`
	AvgSize   uint64    `json:"avg_size"`
	CreatedAt time.Time `json:"created_at"`
	Histogram Histogram `json:"histogram"`
	Columns   []string  `json:"columns"`
	Types     []string  `json:"types"`
	Version   uint16    `json:"version"`
}

func (s *Stats) String() string {
	b := strings.Builder{}
	b.WriteString(fmt.Sprintf("{"))
	sep := ""
	if s.Rows > 0 {
		b.WriteString(fmt.Sprintf("%s\"row_count\": %d", sep, s.Rows))
		sep = ", "
	}
	if s.Distinct > 0 {
		b.WriteString(fmt.Sprintf("%s\"distinct_count\": %d", sep, s.Distinct))
		sep = ", "
	}
	if s.Nulls > 0 {
		b.WriteString(fmt.Sprintf("%s\"null_count\": %d", sep, s.Nulls))
		sep = ", "
	}
	if s.AvgSize > 0 {
		b.WriteString(fmt.Sprintf("%s\"avg_size\": %d", sep, s.AvgSize))
		sep = ", "
	}
	if !s.CreatedAt.IsZero() {
		b.WriteString(fmt.Sprintf("%s\"created_at\": %s", sep, s.CreatedAt))
		sep = ", "
	}
	if len(s.Histogram) > 0 {
		b.WriteString(fmt.Sprintf("%s\"histogram\": %s", sep, s.Histogram))
		sep = ", "
	}
	if len(s.Columns) > 0 {
		var cols []string
		for _, c := range s.Columns {
			cols = append(cols, fmt.Sprintf("\"%s\"", c))
		}
		b.WriteString(fmt.Sprintf("%s\"columns\": [%s]", sep, strings.Join(cols, ",")))
		sep = ", "
	}
	if len(s.Types) > 0 {
		var types []string
		for _, c := range s.Types {
			types = append(types, fmt.Sprintf("\"%s\"", c))
		}
		b.WriteString(fmt.Sprintf("%s\"types\": [%s]", sep, strings.Join(types, ",")))
		sep = ", "
	}

	b.WriteString(fmt.Sprintf("%s\"version\": %d", sep, s.Version))
	b.WriteString("}")
	return b.String()
}

type Histogram []Bucket

type Bucket struct {
	Count      uint64        `json:"count"`
	Distinct   uint64        `json:"distinct"`
	BoundCount uint64        `json:"bound_count"`
	Mcv        []interface{} `json:"mcv"`
	McvCount   []uint64      `json:"mvc_count"`
	UpperBound interface{}   `json:"upper_bound"`
}

func (h Histogram) String() string {
	b := strings.Builder{}
	b.WriteString("[")
	for i, bucket := range h {
		if i > 0 {
			b.WriteString(", {")
		} else {
			b.WriteString("{")
		}
		buckSep := ""
		if bucket.Count > 0 {
			b.WriteString(fmt.Sprintf("%s\"count\": %d", buckSep, bucket.Count))
			buckSep = ", "
		}
		if bucket.Distinct > 0 {
			b.WriteString(fmt.Sprintf("%s\"distinct\": %d", buckSep, bucket.Distinct))
			buckSep = ", "
		}
		if bucket.UpperBound != nil {
			b.WriteString(fmt.Sprintf("%s\"upper_bound\": %v", buckSep, bucket.UpperBound))
			buckSep = ", "
		}
		var mcvs []string
		for _, v := range bucket.Mcv {
			if v != nil {
				mcvs = append(mcvs, fmt.Sprintf("%v", v))
			}
		}
		if len(mcvs) > 0 {
			buckSep = ", "
			b.WriteString(fmt.Sprintf("%s\"mcv\": [%s]", buckSep, strings.Join(mcvs, ", ")))
		}
		var mcvCounts []string
		for _, v := range bucket.McvCount {
			if v > 0 {
				mcvCounts = append(mcvCounts, fmt.Sprintf("%d", v))
			}
		}
		if len(mcvs) > 0 {
			buckSep = ", "
			b.WriteString(fmt.Sprintf("%s\"mcv_count\": [%s]", buckSep, strings.Join(mcvCounts, ", ")))
		}
		b.WriteString("}")
	}
	b.WriteString("]")
	return b.String()
}
