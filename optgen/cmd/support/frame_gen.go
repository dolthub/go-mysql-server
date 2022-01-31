package support

import (
	"fmt"
	"io"
	"math"
	"strings"
)

//go:generate stringer -type=frameExtent

type frameExtent int

const (
	unboundedPreceding frameExtent = iota
	startNPreceding
	startCurrentRow
	startNFollowing
	unknown
	endNPreceding
	endCurrentRow
	endNFollowing
	unboundedFollowing
)

var frameExtents = []frameExtent{
	unboundedPreceding,
	startNPreceding,
	startCurrentRow,
	startNFollowing,
	endNPreceding,
	endCurrentRow,
	endNFollowing,
	unboundedFollowing,
}

func (e frameExtent) argType() string {
	switch e {
	case unboundedPreceding, startCurrentRow, endCurrentRow, unboundedFollowing:
		return "bool"
	case startNPreceding, startNFollowing, endNPreceding, endNFollowing:
		return "sql.Expression"
	}
	panic(fmt.Sprintf("invalid frameExtent: %v", e))
}

func (e frameExtent) Arg() map[string]string {
	return map[string]string{e.String(): e.argType()}
}

func (e frameExtent) cond() string {
	switch e {
	case unboundedPreceding, startCurrentRow, endCurrentRow, unboundedFollowing:
		return fmt.Sprintf("%s", e.String())
	case startNPreceding, startNFollowing, endNPreceding, endNFollowing:
		return fmt.Sprintf("%s != nil", e.String())
	}
	panic(fmt.Sprintf("invalid frameExtent: %v", e))
}

type frameUnit int

const (
	rows frameUnit = iota
	rang
)

var frameUnits = []frameUnit{rows, rang}

func (b frameUnit) String() string {
	switch b {
	case rows:
		return "Rows"
	case rang:
		return "Range"
	}
	return ""
}

type frameBound int

const (
	startBound frameBound = iota
	endBound
)

var implicitRightBound = []frameBound{startBound, endBound}

func (b frameBound) String() string {
	switch b {
	case startBound:
		return "Start"
	case endBound:
		return "End"
	}
	return ""
}

type frameDef struct {
	start frameExtent
	end   frameExtent
	unit  frameUnit
	op    int
}

func (d *frameDef) Name() string {
	start := strings.ReplaceAll(strings.Title(d.start.String()), startBound.String(), "")
	end := strings.ReplaceAll(strings.Title(d.end.String()), endBound.String(), "")
	return fmt.Sprintf("%s%sTo%s", d.unit, start, end)
}

func (d *frameDef) OpName() string {
	return fmt.Sprintf("%sTo%s", d.start.String(), d.end.String())
}

func (d *frameDef) valid() bool {
	switch {
	case d.end == unknown || d.start == unknown:
		return false
	case d.end < d.start:
		return false
	case d.end < unknown:
		return false
	case d.start > unknown:
		return false
	}
	return true
}

func (d *frameDef) Args() []frameExtent {
	return []frameExtent{d.start, d.end}
}

func (d *frameDef) CondArgs() string {
	return fmt.Sprintf("is%s && %s && %s", d.unit, d.start.cond(), d.end.cond())
}

func (d *frameDef) SigArgs() string {
	sb := strings.Builder{}
	i := 0
	for _, a := range d.Args() {
		if a.argType() == "bool" {
			continue
		}
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString(fmt.Sprintf("%s %s", a, a.argType()))
		i++
	}
	return sb.String()
}

type FrameGen struct {
	w     io.Writer
	defs  []frameDef
	limit int
}

func (g *FrameGen) Generate(defines GenDefs, w io.Writer) {
	g.w = w
	if g.limit == 0 {
		g.limit = math.MaxInt32
	}
	g.defs = getDefs(g.limit)
	g.generate()
}

func getDefs(limit int) []frameDef {
	i := 0
	defs := make([]frameDef, 0)
	for _, unit := range frameUnits {
		for _, start := range frameExtents {
			for _, end := range frameExtents {
				def := frameDef{unit: unit, start: start, end: end, op: i}
				if !def.valid() {
					continue
				}
				if i >= limit {
					return defs
				}
				defs = append(defs, def)
				i++
			}
		}
	}
	return defs
}

func (g *FrameGen) generate() {
	g.genImports()
	for _, def := range g.defs {
		g.genFrameType(def)
		g.genNewFrame(def)
		g.genFrameAccessors(def)
		g.genNewFramer(def)
	}
}

func (g *FrameGen) genImports() {
	fmt.Fprintf(g.w, "import (\n")
	fmt.Fprintf(g.w, "  \"github.com/dolthub/go-mysql-server/sql\"\n")
	fmt.Fprintf(g.w, "  agg \"github.com/dolthub/go-mysql-server/sql/expression/function/aggregation\"\n")
	fmt.Fprintf(g.w, ")\n\n")
}

func (g *FrameGen) genFrameType(def frameDef) {
	fmt.Fprintf(g.w, "type %sFrame struct {\n", def.Name())
	fmt.Fprintf(g.w, "    windowFrameBase\n")
	fmt.Fprintf(g.w, "}\n\n")

	fmt.Fprintf(g.w, "var _ sql.WindowFrame = (*%sFrame)(nil)\n\n", def.Name())

}

func (g *FrameGen) genNewFrame(def frameDef) {
	fmt.Fprintf(g.w, "func New%sFrame(%s) *%sFrame {\n", def.Name(), def.SigArgs(), def.Name())
	fmt.Fprintf(g.w, "  return &%sFrame{\n", def.Name())
	fmt.Fprintf(g.w, "    windowFrameBase{\n")
	switch def.unit {
	case rows:
		fmt.Fprintf(g.w, "      isRows: true,\n")
	case rang:
		fmt.Fprintf(g.w, "      isRange: true,\n")
	}

	for _, a := range def.Args() {
		switch a.argType() {
		case "sql.Expression":
			fmt.Fprintf(g.w, "      %s: %s,\n", a, a)
		case "bool":
			fmt.Fprintf(g.w, "      %s: true,\n", a)
		}
	}

	fmt.Fprintf(g.w, "    },\n")
	fmt.Fprintf(g.w, "  }\n")
	fmt.Fprintf(g.w, "}\n\n")
}

func (g *FrameGen) genFrameAccessors(def frameDef) {
	for _, e := range frameExtents {
		fmt.Fprintf(g.w, "func (f *%sFrame) %s() %s {\n", def.Name(), strings.Title(e.String()), e.argType())
		fmt.Fprintf(g.w, "  return f.%s\n", e)
		fmt.Fprintf(g.w, "}\n\n")
	}
}

func (g *FrameGen) genNewFramer(def frameDef) {
	fmt.Fprintf(g.w, "func (f *%sFrame) NewFramer(w *sql.Window) (sql.WindowFramer, error) {\n", def.Name())
	fmt.Fprintf(g.w, "    return agg.New%sFramer(f, w)\n", def.Name())
	fmt.Fprintf(g.w, "}\n\n")
}
