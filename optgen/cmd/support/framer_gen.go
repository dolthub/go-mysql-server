package support

import (
	"fmt"
	"io"
	"math"
	"strings"
)

type FramerGen struct {
	w     io.Writer
	defs  []frameDef
	limit int
}

func (g *FramerGen) Generate(defines GenDefs, w io.Writer) {
	g.w = w
	if g.limit == 0 {
		g.limit = math.MaxInt32
	}
	g.defs = getDefs(g.limit)
	g.generate()
}

func (g *FramerGen) generate() {
	g.genImports()
	for _, def := range g.defs {
		g.genFramerType(def)
		g.genNewFramer(def)
	}
}

func (g *FramerGen) genImports() {
	fmt.Fprintf(g.w, "import (\n")
	fmt.Fprintf(g.w, "  \"github.com/gabereiser/go-mysql-server/sql\"\n")
	fmt.Fprintf(g.w, "  \"github.com/gabereiser/go-mysql-server/sql/expression\"\n")
	fmt.Fprintf(g.w, ")\n\n")
}

func (g *FramerGen) genFramerType(def frameDef) {
	fmt.Fprintf(g.w, "type %sFramer struct {\n", def.Name())
	switch def.unit {
	case rows:
		fmt.Fprintf(g.w, "  rowFramerBase\n")
	case rang:
		fmt.Fprintf(g.w, "  rangeFramerBase\n")
	}
	fmt.Fprintf(g.w, "}\n\n")

	fmt.Fprintf(g.w, "var _ sql.WindowFramer = (*%sFramer)(nil)\n\n", def.Name())

}

func (g *FramerGen) genNewFramer(def frameDef) {
	framerName := fmt.Sprintf("%sFramer", def.Name())
	fmt.Fprintf(g.w, "func New%sFramer(frame sql.WindowFrame, window *sql.WindowDefinition) (sql.WindowFramer, error) {\n", def.Name())

	for _, a := range def.Args() {
		switch a.argType() {
		case "sql.Expression":
			switch def.unit {
			case rows:
				fmt.Fprintf(g.w, "  %s, err := expression.LiteralToInt(frame.%s())\n", a, strings.Title(a.String()))
				fmt.Fprintf(g.w, "  if err != nil {\n")
				fmt.Fprintf(g.w, "    return nil, err\n")
				fmt.Fprintf(g.w, "  }\n")
			case rang:
				fmt.Fprintf(g.w, "  %s := frame.%s()\n", a, strings.Title(a.String()))
			}
		case "bool":
			fmt.Fprintf(g.w, "  %s := true\n", a)
		}
	}

	orderByRequired := def.unit == rang &&
		((def.start != unboundedPreceding && def.start != startCurrentRow) ||
			(def.end != unboundedFollowing && def.end != endCurrentRow))

	if orderByRequired {
		fmt.Fprintf(g.w, "  if len(window.OrderBy) != 1 {\n")
		fmt.Fprintf(g.w, "    return nil, ErrRangeInvalidOrderBy.New(len(window.OrderBy.ToExpressions()))\n")
		fmt.Fprintf(g.w, "  }\n")
	}

	if def.unit == rang {
		fmt.Fprintf(g.w, "  var orderBy sql.Expression\n")
		fmt.Fprintf(g.w, "  if len(window.OrderBy) > 0 {\n")
		fmt.Fprintf(g.w, "    orderBy = window.OrderBy.ToExpressions()[0]\n")
		fmt.Fprintf(g.w, "  }\n")
	}

	fmt.Fprintf(g.w, "  return &%s{\n", framerName)
	switch def.unit {
	case rows:
		fmt.Fprintf(g.w, "    rowFramerBase{\n")
	case rang:
		fmt.Fprintf(g.w, "    rangeFramerBase{\n")
		fmt.Fprintf(g.w, "      orderBy: orderBy,\n")
	}

	for _, a := range def.Args() {
		fmt.Fprintf(g.w, "      %s: %s,\n", a, a)
	}

	fmt.Fprintf(g.w, "    },\n")
	fmt.Fprintf(g.w, "  }, nil\n")
	fmt.Fprintf(g.w, "}\n\n")
}
