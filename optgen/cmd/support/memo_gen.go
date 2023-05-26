package support

import (
	"bytes"
	"fmt"
	"gopkg.in/yaml.v3"
	"io"
	"os"
	"strings"
)

//go:generate go run ../optgen/main.go -out ../../../sql/analyzer/memo.og.go -pkg analyzer memo

type MemoExprs struct {
	Exprs []ExprDef `yaml:"exprs"`
}

type ExprDef struct {
	Name       string      `yaml:"name"`
	SourceType string      `yaml:"sourceType"`
	Join       bool        `yaml:"join"`
	Attrs      [][2]string `yaml:"attrs"`
	Unary      bool        `yaml:"unary"`
	SkipExec   bool        `yaml:"skipExec"`
	Scalar     bool        `yaml:"scalar"`
	Binary     bool        `yaml:"binary"`
}

func DecodeMemoExprs(path string) (MemoExprs, error) {
	contents, err := os.ReadFile(path)
	if err != nil {
		return MemoExprs{}, err
	}
	dec := yaml.NewDecoder(bytes.NewReader(contents))
	dec.KnownFields(true)
	var res MemoExprs
	return res, dec.Decode(&res)
}

var _ GenDefs = (*MemoExprs)(nil)

type MemoGen struct {
	defines []ExprDef
	w       io.Writer
}

func (g *MemoGen) Generate(defines GenDefs, w io.Writer) {
	g.defines = defines.(MemoExprs).Exprs

	g.w = w

	g.genImport()
	for _, define := range g.defines {
		g.genType(define)
		if define.Scalar {
			g.genScalarInterfaces(define)
		} else {
			g.genRelInterfaces(define)
		}
		g.genStringer(define)
		if define.SourceType != "" {
			g.genSourceRelInterface(define)
		}
		if define.Join {
			g.genJoinRelInterface(define)
		} else if define.Binary {
			g.genBinaryGroupInterface(define)
		} else if define.Unary {
			g.genUnaryGroupInterface(define)
		} else {
			g.genChildlessGroupInterface(define)
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

func (g *MemoGen) genType(define ExprDef) {
	fmt.Fprintf(g.w, "type %s struct {\n", define.Name)
	if define.SourceType != "" {
		fmt.Fprintf(g.w, "  *relBase\n")
		fmt.Fprintf(g.w, "  table %s\n", define.SourceType)
	} else if define.Join {
		fmt.Fprintf(g.w, "  *joinBase\n")
	} else if define.Unary {
		fmt.Fprintf(g.w, "  *relBase\n")
		fmt.Fprintf(g.w, "  child *exprGroup\n")
	} else if define.Binary {
		if define.Scalar {
			fmt.Fprintf(g.w, "  *scalarBase\n")
		} else {
			fmt.Fprintf(g.w, "  *relBase\n")
		}
		fmt.Fprintf(g.w, "  left *exprGroup\n")
		fmt.Fprintf(g.w, "  right *exprGroup\n")
	} else if define.Scalar {
		fmt.Fprintf(g.w, "  *scalarBase\n")
	}
	for _, attr := range define.Attrs {
		fmt.Fprintf(g.w, "  %s %s\n", attr[0], attr[1])
	}

	fmt.Fprintf(g.w, "}\n\n")
}

func (g *MemoGen) genRelInterfaces(define ExprDef) {
	fmt.Fprintf(g.w, "var _ relExpr = (*%s)(nil)\n", define.Name)
	if define.SourceType != "" {
		fmt.Fprintf(g.w, "var _ sourceRel = (*%s)(nil)\n", define.Name)
	} else if define.Join {
		fmt.Fprintf(g.w, "var _ joinRel = (*%s)(nil)\n", define.Name)
	} else if define.Unary || define.Binary {
	} else {
		panic("unreachable")
	}
	fmt.Fprintf(g.w, "\n")
}

func (g *MemoGen) genScalarInterfaces(define ExprDef) {
	fmt.Fprintf(g.w, "var _ scalarExpr = (*%s)(nil)\n", define.Name)
	
	fmt.Fprintf(g.w, "\n")

	fmt.Fprintf(g.w, "func (r *%s) exprId() scalarExprId {\n", define.Name)
	fmt.Fprintf(g.w, "  return %sExpr\n", define.Name)
	fmt.Fprintf(g.w, "}\n\n")
}

func (g *MemoGen) genStringer(define ExprDef) {
	fmt.Fprintf(g.w, "func (r *%s) String() string {\n", define.Name)
	fmt.Fprintf(g.w, "  return formatExpr(r)\n")
	fmt.Fprintf(g.w, "}\n\n")
}

func (g *MemoGen) genSourceRelInterface(define ExprDef) {
	fmt.Fprintf(g.w, "func (r *%s) name() string {\n", define.Name)
	fmt.Fprintf(g.w, "  return strings.ToLower(r.table.Name())\n")
	fmt.Fprintf(g.w, "}\n\n")

	fmt.Fprintf(g.w, "func (r *%s) tableId() TableId {\n", define.Name)
	fmt.Fprintf(g.w, "  return tableIdForSource(r.g.id)\n")
	fmt.Fprintf(g.w, "}\n\n")

	fmt.Fprintf(g.w, "func (r *%s) outputCols() sql.Schema {\n", define.Name)
	fmt.Fprintf(g.w, "  return r.table.Schema()\n")
	fmt.Fprintf(g.w, "}\n\n")
}

func (g *MemoGen) genJoinRelInterface(define ExprDef) {
	fmt.Fprintf(g.w, "func (r *%s) joinPrivate() *joinBase {\n", define.Name)
	fmt.Fprintf(g.w, "  return r.joinBase\n")
	fmt.Fprintf(g.w, "}\n\n")
}

func (g *MemoGen) genBinaryGroupInterface(define ExprDef) {
	fmt.Fprintf(g.w, "func (r *%s) children() []*exprGroup {\n", define.Name)
	fmt.Fprintf(g.w, "  return []*exprGroup{r.left, r.right}\n")
	fmt.Fprintf(g.w, "}\n\n")
}

func (g *MemoGen) genChildlessGroupInterface(define ExprDef) {
	fmt.Fprintf(g.w, "func (r *%s) children() []*exprGroup {\n", define.Name)
	fmt.Fprintf(g.w, "  return nil\n")
	fmt.Fprintf(g.w, "}\n\n")
}

func (g *MemoGen) genUnaryGroupInterface(define ExprDef) {
	fmt.Fprintf(g.w, "func (r *%s) children() []*exprGroup {\n", define.Name)
	fmt.Fprintf(g.w, "  return []*exprGroup{r.child}\n")
	fmt.Fprintf(g.w, "}\n\n")

	fmt.Fprintf(g.w, "func (r *%s) outputCols() sql.Schema {\n", define.Name)
	switch define.Name {
	case "project":
		fmt.Fprintf(g.w, "  var s = make(sql.Schema, len(r.projections))\n")
		fmt.Fprintf(g.w, "  for i, e := range r.projections {\n")
		fmt.Fprintf(g.w, "    ref := e.scalar.(*colRef)\n")
		fmt.Fprintf(g.w, "    s[i] = &sql.Column{\n")
		fmt.Fprintf(g.w, "      Name:     ref.gf.Name(),\n")
		fmt.Fprintf(g.w, "      Source:   ref.gf.String(),\n")
		fmt.Fprintf(g.w, "      Type:     ref.gf.Type(),\n")
		fmt.Fprintf(g.w, "      Nullable: ref.gf.IsNullable(),\n")
		fmt.Fprintf(g.w, "    }\n")
		fmt.Fprintf(g.w, "  }\n")
		fmt.Fprintf(g.w, "  return s\n")

	default:
		fmt.Fprintf(g.w, "  return r.child.relProps.OutputCols()\n")
	}

	fmt.Fprintf(g.w, "}\n\n")

}

func (g *MemoGen) genFormatters(defines []ExprDef) {
	// printer
	fmt.Fprintf(g.w, "func formatExpr(r exprType) string {\n")
	fmt.Fprintf(g.w, "  switch r := r.(type) {\n")
	for _, d := range defines {
		fmt.Fprintf(g.w, "  case *%s:\n", d.Name)
		if d.SourceType != "" {
			fmt.Fprintf(g.w, "    return fmt.Sprintf(\"%s: %%s\", r.name())\n", d.Name)
		} else if d.Join || d.Binary {
			fmt.Fprintf(g.w, "    return fmt.Sprintf(\"%s %%d %%d\", r.left.id, r.right.id)\n", d.Name)
		} else if d.Unary {
			fmt.Fprintf(g.w, "    return fmt.Sprintf(\"%s: %%d\", r.child.id)\n", d.Name)
		} else if d.Scalar {
			attrs := ""
			sep := ": "
			ret := fmt.Sprintf("    return fmt.Sprintf(\"%s", d.Name)
			for _, attr := range d.Attrs {
				attrs += fmt.Sprintf("%sr.%s", sep, attr[0])
				sep = ", "
			}
			ret += "\")\n"
			fmt.Fprintf(g.w, ret)
		} else {
			panic("unreachable")
		}
	}
	fmt.Fprintf(g.w, "  default:\n")
	fmt.Fprintf(g.w, "    panic(fmt.Sprintf(\"unknown relExpr type: %%T\", r))\n")
	fmt.Fprintf(g.w, "  }\n")
	fmt.Fprintf(g.w, "}\n\n")

	// to sqlNode
	fmt.Fprintf(g.w, "func buildRelExpr(b *ExecBuilder, r relExpr, input sql.Schema, children ...sql.Node) (sql.Node, error) {\n")
	fmt.Fprintf(g.w, "  var result sql.Node\n")
	fmt.Fprintf(g.w, "  var err error\n\n")
	fmt.Fprintf(g.w, "  switch r := r.(type) {\n")
	for _, d := range defines {
		if d.SkipExec || d.Scalar {
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

	// to sqlExpr
	fmt.Fprintf(g.w, "func buildScalarExpr(b *ExecBuilder, r scalarExpr, sch sql.Schema) (sql.Expression, error) {\n")
	fmt.Fprintf(g.w, "  switch r := r.(type) {\n")
	for _, d := range defines {
		if d.SkipExec || !d.Scalar {
			continue
		}
		fmt.Fprintf(g.w, "  case *%s:\n", d.Name)
		fmt.Fprintf(g.w, "  return b.build%s(r, sch)\n", strings.Title(d.Name))
	}
	fmt.Fprintf(g.w, "  default:\n")
	fmt.Fprintf(g.w, "    panic(fmt.Sprintf(\"unknown scalarExpr type: %%T\", r))\n")
	fmt.Fprintf(g.w, "  }\n")
	fmt.Fprintf(g.w, "}\n\n")
}
