package support

import (
	"bytes"
	"fmt"
	"strings"
	"testing"
)

func TestFrameGen(t *testing.T) {
	test := struct {
		expected string
	}{
		expected: `
		import (
		  "github.com/dolthub/go-mysql-server/sql"
		  agg "github.com/dolthub/go-mysql-server/sql/expression/function/aggregation"
		)
		
		type RowsUnboundedPrecedingToNPrecedingFrame struct {
			windowFrameBase
		}
		
		var _ sql.WindowFrame = (*RowsUnboundedPrecedingToNPrecedingFrame)(nil)
		
		func NewRowsUnboundedPrecedingToNPrecedingFrame(endNPreceding sql.Expression) *RowsUnboundedPrecedingToNPrecedingFrame {
		  return &RowsUnboundedPrecedingToNPrecedingFrame{
			windowFrameBase{
			  isRows: true,
			  unboundedPreceding: true,
			  endNPreceding: endNPreceding,
			},
		  }
		}
		
		func (f *RowsUnboundedPrecedingToNPrecedingFrame) UnboundedPreceding() bool {
		  return f.unboundedPreceding
		}
		
		func (f *RowsUnboundedPrecedingToNPrecedingFrame) StartNPreceding() sql.Expression {
		  return f.startNPreceding
		}
		
		func (f *RowsUnboundedPrecedingToNPrecedingFrame) StartCurrentRow() bool {
		  return f.startCurrentRow
		}
		
		func (f *RowsUnboundedPrecedingToNPrecedingFrame) StartNFollowing() sql.Expression {
		  return f.startNFollowing
		}
		
		func (f *RowsUnboundedPrecedingToNPrecedingFrame) EndNPreceding() sql.Expression {
		  return f.endNPreceding
		}
		
		func (f *RowsUnboundedPrecedingToNPrecedingFrame) EndCurrentRow() bool {
		  return f.endCurrentRow
		}
		
		func (f *RowsUnboundedPrecedingToNPrecedingFrame) EndNFollowing() sql.Expression {
		  return f.endNFollowing
		}
		
		func (f *RowsUnboundedPrecedingToNPrecedingFrame) UnboundedFollowing() bool {
		  return f.unboundedFollowing
		}
		
		func (f *RowsUnboundedPrecedingToNPrecedingFrame) NewFramer(w *sql.Window) (sql.WindowFramer, error) {
			return agg.NewRowsUnboundedPrecedingToNPrecedingFramer(f, w)
		}
		`,
	}

	gen := FrameGen{limit: 1}
	var buf bytes.Buffer
	gen.Generate(nil, &buf)

	if testing.Verbose() {
		fmt.Printf("\n=>\n\n%s\n", buf.String())
	}

	if !strings.Contains(removeWhitespace(buf.String()), removeWhitespace(test.expected)) {
		t.Fatalf("\nexpected:\n%s\nactual:\n%s", test.expected, buf.String())
	}
}
