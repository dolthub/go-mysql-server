package plan

import (
	"fmt"

	"github.com/dolthub/go-mysql-server/sql"
	agg "github.com/dolthub/go-mysql-server/sql/expression/function/aggregation"
)

//go:generate go run ../../optgen/cmd/optgen/main.go -out window_frame.og.go -pkg plan frame window_frame.go

// windowFrameBase maintains window frame bounds if
// set for a plan.Window's over clause.
// If no bounds set, plan.Window uses a nil windowFrameBase.
// We assume the parser errors for bound pairings that
// violate PRECEDING < CURRENT ROW < FOLLOWING ordering,
// and only a valid subset of fields will be set.
type windowFrameBase struct {
	startNPreceding sql.Expression
	startNFollowing sql.Expression
	endNPreceding   sql.Expression
	endNFollowing   sql.Expression

	isRows             bool
	isRange            bool
	unboundedFollowing bool
	unboundedPreceding bool
	startCurrentRow    bool
	endCurrentRow      bool
}

func (f *windowFrameBase) String() string {
	if f == nil {
		return ""
	}

	var boundType string
	if f.isRange {
		boundType = "RANGE"
	} else {
		boundType = "ROWS"
	}

	var startExtent string
	switch {
	case f.unboundedPreceding:
		startExtent = "UNBOUNDED PRECEDING"
	case f.startCurrentRow:
		startExtent = "CURRENT ROW"
	case f.startNFollowing != nil:
		startExtent = fmt.Sprintf("%s FOLLOWING", f.startNFollowing.String())
	case f.startNPreceding != nil:
		startExtent = fmt.Sprintf("%s PRECEDING", f.startNPreceding.String())
	default:
	}

	var endExtent string
	switch {
	case f.unboundedFollowing:
		endExtent = "UNBOUNDED FOLLOWING"
	case f.endCurrentRow:
		endExtent = "CURRENT ROW"
	case f.endNFollowing != nil:
		endExtent = fmt.Sprintf("%s FOLLOWING", f.endNFollowing.String())
	case f.endNPreceding != nil:
		endExtent = fmt.Sprintf("%s PRECEDING", f.endNPreceding.String())
	default:
	}

	if endExtent != "" {
		return fmt.Sprintf("%s BETWEEN %s AND %s", boundType, startExtent, endExtent)
	} else {
		return fmt.Sprintf("%s %s", boundType, startExtent)
	}
}

func (f *windowFrameBase) DebugString(ctx *sql.Context) string {
	if f == nil {
		return ""
	}
	return f.String()
}

// ExcludingWindowFrame is a sql.WindowFrame with an EXCLUDE clause, which removes rows around the current row from
// the frames produced by the wrapped sql.WindowFrame.
type ExcludingWindowFrame struct {
	sql.WindowFrame
	Exclusion sql.WindowFrameExclusion
}

var _ sql.WindowFrame = (*ExcludingWindowFrame)(nil)

// NewExcludingWindowFrame returns |frame| with the given |exclusion| applied. EXCLUDE NO OTHERS is the default, so
// |frame| is returned unchanged in that case.
func NewExcludingWindowFrame(frame sql.WindowFrame, exclusion sql.WindowFrameExclusion) sql.WindowFrame {
	if frame == nil || exclusion == sql.WindowFrameExcludeNoOthers {
		return frame
	}
	return &ExcludingWindowFrame{
		WindowFrame: frame,
		Exclusion:   exclusion,
	}
}

// String implements fmt.Stringer
func (f *ExcludingWindowFrame) String() string {
	return fmt.Sprintf("%s %s", f.WindowFrame.String(), f.Exclusion.String())
}

// DebugString implements sql.DebugStringer
func (f *ExcludingWindowFrame) DebugString(ctx *sql.Context) string {
	return f.String()
}

// NewFramer implements sql.WindowFrame
func (f *ExcludingWindowFrame) NewFramer(w *sql.WindowDefinition) (sql.WindowFramer, error) {
	framer, err := f.WindowFrame.NewFramer(w)
	if err != nil {
		return nil, err
	}

	return agg.NewExcludingFramer(framer, f.Exclusion, w.OrderBy.ToExpressions()), nil
}
