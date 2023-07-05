package support

import (
	"bytes"
	"fmt"
	"strings"
	"testing"
)

func TestFrameFactoryGen(t *testing.T) {
	test := struct {
		expected string
	}{
		expected: `
import (
  "fmt"
  "github.com/dolthub/go-mysql-server/sql"
  "github.com/dolthub/go-mysql-server/sql/plan"
  ast "github.com/dolthub/vitess/go/vt/sqlparser"
)

func (b *PlanBuilder) NewFrame(inScope *scope, f *ast.Frame) sql.WindowFrame {
  if f == nil {
    return nil
  }
  isRange := f.Unit == ast.RangeUnit
  isRows := f.Unit == ast.RowsUnit
  unboundedPreceding := b.getFrameUnboundedPreceding(inScope, f)
  startNPreceding := b.getFrameStartNPreceding(inScope, f)
  startCurrentRow := b.getFrameStartCurrentRow(inScope, f)
  startNFollowing := b.getFrameStartNFollowing(inScope, f)
  endNPreceding := b.getFrameEndNPreceding(inScope, f)
  endCurrentRow := b.getFrameEndCurrentRow(inScope, f)
  endNFollowing := b.getFrameEndNFollowing(inScope, f)
  unboundedFollowing := b.getFrameUnboundedFollowing(inScope, f)
  switch {
  case isRows && unboundedPreceding && endNPreceding != nil:
    return plan.NewRowsUnboundedPrecedingToNPrecedingFrame(endNPreceding)
  default:
    err := fmt.Errorf("no matching constructor found for frame: %v", f)
    b.handleErr(err)
    return nil
  }
}
		`,
	}

	gen := FrameFactoryGen{limit: 1}
	var buf bytes.Buffer
	gen.Generate(nil, &buf)

	if testing.Verbose() {
		fmt.Printf("\n=>\n\n%s\n", buf.String())
	}

	if !strings.Contains(removeWhitespace(buf.String()), removeWhitespace(test.expected)) {
		t.Fatalf("\nexpected:\n%s\nactual:\n%s", test.expected, buf.String())
	}
}
