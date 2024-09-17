package sql

import "fmt"

type OrderAndLimit struct {
	OrderBy       Expression
	Limit         Expression
	CalcFoundRows bool
}

func (v OrderAndLimit) DebugString() string {
	if v.Limit != nil {
		return fmt.Sprintf("%v LIMIT %v", DebugString(v.OrderBy), DebugString(v.Limit))
	}
	return DebugString(v.OrderBy)
}

func (v OrderAndLimit) String() string {
	if v.Limit != nil {
		return fmt.Sprintf("%v LIMIT %v", v.OrderBy, v.Limit)
	}
	return v.OrderBy.String()
}
