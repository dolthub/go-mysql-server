package support

import (
	"fmt"
	"io"
	"strings"
)

type MemoDef struct {
	Name string

	SourceType string

	IsJoin bool
	Attrs  [][2]string

	IsUnary  bool
	SkipExec bool
}

var _ GenDefs = ([]MemoDef)(nil)

type MemoGen struct {
	defines []MemoDef
	w       io.Writer
}

func (g *MemoGen) Generate(defines GenDefs, w io.Writer) {
	g.defines = defines.([]MemoDef)

	g.w = w

	g.genImport()
	for _, define := range g.defines {
		g.genType(define)
		g.genInterfaces(define)
		g.genStringer(define)
		g.genRelExprInterface(define)
		if define.SourceType != "" {
			g.genSourceRelInterface(define)
		}
		if define.IsJoin {
			g.genJoinRelInterface(define)
		}
		if define.IsUnary {
			g.genUnaryRelInterface(define)
		}
	}
	g.genFormatters(g.defines)

}

func (g *MemoGen) genImport() {
	fmt.Fprintf(g.w, "import (\n")
	fmt.Fprintf(g.w, "  \"fmt\"\n")
	fmt.Fprintf(g.w, "  \"strings\"\n")
	fmt.Fprintf(g.w, "  \"github.com/dolthub/go-mysql-server/sql\"\n")
	fmt.Fprintf(g.w, "  \"github.com/dolthub/go-mysql-server/sql/plan\"\n")
	fmt.Fprintf(g.w, ")\n\n")
}

func (g *MemoGen) genType(define MemoDef) {
	fmt.Fprintf(g.w, "type %s struct {\n", define.Name)
	if define.SourceType != "" {
		fmt.Fprintf(g.w, "  *relBase\n")
		fmt.Fprintf(g.w, "  table %s\n", define.SourceType)
	} else if define.IsJoin {
		fmt.Fprintf(g.w, "  *joinBase\n")
	} else if define.IsUnary {
		fmt.Fprintf(g.w, "  *relBase\n")
		fmt.Fprintf(g.w, "  child *exprGroup\n")
	}
	for _, attr := range define.Attrs {
		fmt.Fprintf(g.w, "  %s %s\n", attr[0], attr[1])
	}

	fmt.Fprintf(g.w, "}\n\n")
}

func (g *MemoGen) genInterfaces(define MemoDef) {
	fmt.Fprintf(g.w, "var _ relExpr = (*%s)(nil)\n", define.Name)
	if define.SourceType != "" {
		fmt.Fprintf(g.w, "var _ sourceRel = (*%s)(nil)\n", define.Name)
	} else if define.IsJoin {
		fmt.Fprintf(g.w, "var _ joinRel = (*%s)(nil)\n", define.Name)
	} else if define.IsUnary {

	} else {
		panic("unreachable")
	}
	fmt.Fprintf(g.w, "\n")
}

func (g *MemoGen) genStringer(define MemoDef) {
	fmt.Fprintf(g.w, "func (r *%s) String() string {\n", define.Name)
	fmt.Fprintf(g.w, "  return formatRelExpr(r)\n")
	fmt.Fprintf(g.w, "}\n\n")
}

func (g *MemoGen) genRelExprInterface(define MemoDef) {
}

func (g *MemoGen) genSourceRelInterface(define MemoDef) {
	fmt.Fprintf(g.w, "func (r *%s) name() string {\n", define.Name)
	fmt.Fprintf(g.w, "  return strings.ToLower(r.table.Name())\n")
	fmt.Fprintf(g.w, "}\n\n")

	fmt.Fprintf(g.w, "func (r *%s) tableId() TableId {\n", define.Name)
	fmt.Fprintf(g.w, "  return tableIdForSource(r.g.id)\n")
	fmt.Fprintf(g.w, "}\n\n")

	fmt.Fprintf(g.w, "func (r *%s) children() []*exprGroup {\n", define.Name)
	fmt.Fprintf(g.w, "  return nil\n")
	fmt.Fprintf(g.w, "}\n\n")

	fmt.Fprintf(g.w, "func (r *%s) outputCols() sql.Schema {\n", define.Name)
	fmt.Fprintf(g.w, "  return r.table.Schema()\n")
	fmt.Fprintf(g.w, "}\n\n")
}

func (g *MemoGen) genJoinRelInterface(define MemoDef) {
	fmt.Fprintf(g.w, "func (r *%s) joinPrivate() *joinBase {\n", define.Name)
	fmt.Fprintf(g.w, "  return r.joinBase\n")
	fmt.Fprintf(g.w, "}\n\n")
}

func (g *MemoGen) genUnaryRelInterface(define MemoDef) {
	fmt.Fprintf(g.w, "func (r *%s) children() []*exprGroup {\n", define.Name)
	fmt.Fprintf(g.w, "  return []*exprGroup{r.child}\n")
	fmt.Fprintf(g.w, "}\n\n")

	fmt.Fprintf(g.w, "func (r *%s) outputCols() sql.Schema {\n", define.Name)
	switch define.Name {
	case "project":
		fmt.Fprintf(g.w, "  var s = make(sql.Schema, len(r.projections))\n")
		fmt.Fprintf(g.w, "  for i, e := range r.projections {\n")
		fmt.Fprintf(g.w, "    s[i] = transform.ExpressionToColumn(e)\n")
		fmt.Fprintf(g.w, "  }\n")
		fmt.Fprintf(g.w, "  return s\n")

	default:
		fmt.Fprintf(g.w, "  return r.child.relProps.OutputCols()\n")
	}
	fmt.Fprintf(g.w, "}\n\n")

}

func (g *MemoGen) genFormatters(defines []MemoDef) {
	fmt.Fprintf(g.w, "func formatRelExpr(r relExpr) string {\n")
	fmt.Fprintf(g.w, "  switch r := r.(type) {\n")
	for _, d := range defines {
		fmt.Fprintf(g.w, "  case *%s:\n", d.Name)
		if d.SourceType != "" {
			fmt.Fprintf(g.w, "    return fmt.Sprintf(\"%s: %%s\", r.name())\n", d.Name)
		} else if d.IsJoin {
			fmt.Fprintf(g.w, "    return fmt.Sprintf(\"%s %%d %%d\", r.left.id, r.right.id)\n", d.Name)
		} else if d.IsUnary {
			fmt.Fprintf(g.w, "    return fmt.Sprintf(\"%s: %%d\", r.child.id)\n", d.Name)
		} else {
			panic("unreachable")
		}
	}
	fmt.Fprintf(g.w, "  default:\n")
	fmt.Fprintf(g.w, "    panic(fmt.Sprintf(\"unknown relExpr type: %%T\", r))\n")
	fmt.Fprintf(g.w, "  }\n")
	fmt.Fprintf(g.w, "}\n\n")

	fmt.Fprintf(g.w, "func buildRelExpr(b *ExecBuilder, r relExpr, input sql.Schema, children ...sql.Node) (sql.Node, error) {\n")
	fmt.Fprintf(g.w, "  var result sql.Node\n")
	fmt.Fprintf(g.w, "  var err error\n\n")
	fmt.Fprintf(g.w, "  switch r := r.(type) {\n")
	for _, d := range defines {
		if d.SkipExec {
			continue
		}
		fmt.Fprintf(g.w, "  case *%s:\n", d.Name)
		fmt.Fprintf(g.w, "  result, err = b.build%s(r, input, children...)\n", strings.Title(d.Name))
	}
	fmt.Fprintf(g.w, "  default:\n")
	fmt.Fprintf(g.w, "    panic(fmt.Sprintf(\"unknown relExpr type: %%T\", r))\n")
	fmt.Fprintf(g.w, "  }\n\n")
	fmt.Fprintf(g.w, "  if err != nil {\n")
	fmt.Fprintf(g.w, "    return nil, err\n")
	fmt.Fprintf(g.w, "  }\n\n")
	fmt.Fprintf(g.w, "  result, err = r.group().finalize(result, input)\n")
	fmt.Fprintf(g.w, "  if err != nil {\n")
	fmt.Fprintf(g.w, "    return nil, err\n")
	fmt.Fprintf(g.w, "  }\n")
	fmt.Fprintf(g.w, "  return result, nil\n")

	fmt.Fprintf(g.w, "}\n\n")
}
