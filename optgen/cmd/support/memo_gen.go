package support

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"strings"

	"gopkg.in/yaml.v3"
)

//go:generate go run ../optgen/main.go -out ../../../sql/memo/memo.og.go -pkg memo memo

type MemoExprs struct {
	Exprs []ExprDef `yaml:"exprs"`
}

type ExprDef struct {
	Name        string      `yaml:"name"`
	SourceType  string      `yaml:"sourceType"`
	Join        bool        `yaml:"join"`
	Attrs       [][2]string `yaml:"attrs"`
	Unary       bool        `yaml:"unary"`
	SkipExec    bool        `yaml:"skipExec"`
	Binary      bool        `yaml:"binary"`
	SkipName    bool        `yaml:"skipName"`
	SkipTableId bool        `yaml:"skipTableId"`
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
		g.genRelInterfaces(define)

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
	fmt.Fprintf(g.w, "type %s struct {\n", strings.Title(define.Name))
	if define.SourceType != "" {
		fmt.Fprintf(g.w, "  *sourceBase\n")
		fmt.Fprintf(g.w, "  Table %s\n", define.SourceType)
	} else if define.Join {
		fmt.Fprintf(g.w, "  *JoinBase\n")
	} else if define.Unary {
		fmt.Fprintf(g.w, "  *relBase\n")
		fmt.Fprintf(g.w, "  Child *ExprGroup\n")
	} else if define.Binary {
		fmt.Fprintf(g.w, "  *relBase\n")
		fmt.Fprintf(g.w, "  Left *ExprGroup\n")
		fmt.Fprintf(g.w, "  Right *ExprGroup\n")
	}
	for _, attr := range define.Attrs {
		fmt.Fprintf(g.w, "  %s %s\n", strings.Title(attr[0]), attr[1])
	}

	fmt.Fprintf(g.w, "}\n\n")
}

func (g *MemoGen) genRelInterfaces(define ExprDef) {
	fmt.Fprintf(g.w, "var _ RelExpr = (*%s)(nil)\n", define.Name)
	if define.SourceType != "" {
		fmt.Fprintf(g.w, "var _ SourceRel = (*%s)(nil)\n", define.Name)
	} else if define.Join {
		fmt.Fprintf(g.w, "var _ JoinRel = (*%s)(nil)\n", define.Name)
	} else if define.Unary || define.Binary {
	} else {
		panic("unreachable")
	}
	fmt.Fprintf(g.w, "\n")
}

func (g *MemoGen) genScalarInterfaces(define ExprDef) {
	fmt.Fprintf(g.w, "var _ ScalarExpr = (*%s)(nil)\n", define.Name)

	fmt.Fprintf(g.w, "\n")

	fmt.Fprintf(g.w, "func (r *%s) ExprId() ScalarExprId {\n", define.Name)
	fmt.Fprintf(g.w, "  return ScalarExpr%s\n", strings.Title(define.Name))
	fmt.Fprintf(g.w, "}\n\n")
}

func (g *MemoGen) genStringer(define ExprDef) {
	fmt.Fprintf(g.w, "func (r *%s) String() string {\n", define.Name)
	fmt.Fprintf(g.w, "  return FormatExpr(r)\n")
	fmt.Fprintf(g.w, "}\n\n")
}

func (g *MemoGen) genSourceRelInterface(define ExprDef) {
	fmt.Fprintf(g.w, "func (r *%s) Name() string {\n", define.Name)
	if !define.SkipName {
		fmt.Fprintf(g.w, "  return strings.ToLower(r.Table.Name())\n")
	} else {
		fmt.Fprintf(g.w, "  return \"\"\n")
	}
	fmt.Fprintf(g.w, "}\n\n")

	fmt.Fprintf(g.w, "func (r *%s) TableId() sql.TableId {\n", define.Name)
	fmt.Fprintf(g.w, "  return TableIdForSource(r.g.Id)\n")
	fmt.Fprintf(g.w, "}\n\n")

	fmt.Fprintf(g.w, "func (r *%s) TableIdNode() plan.TableIdNode {\n", define.Name)
	if define.SkipTableId {
		fmt.Fprintf(g.w, "  return nil\n")
	} else {
		fmt.Fprintf(g.w, "  return r.Table\n")
	}
	fmt.Fprintf(g.w, "}\n\n")

	fmt.Fprintf(g.w, "func (r *%s) OutputCols() sql.Schema {\n", define.Name)
	fmt.Fprintf(g.w, "  return r.Table.Schema()\n")
	fmt.Fprintf(g.w, "}\n\n")
}

func (g *MemoGen) genJoinRelInterface(define ExprDef) {
	fmt.Fprintf(g.w, "func (r *%s) JoinPrivate() *JoinBase {\n", define.Name)
	fmt.Fprintf(g.w, "  return r.JoinBase\n")
	fmt.Fprintf(g.w, "}\n\n")
}

func (g *MemoGen) genBinaryGroupInterface(define ExprDef) {
	fmt.Fprintf(g.w, "func (r *%s) Children() []*ExprGroup {\n", define.Name)
	fmt.Fprintf(g.w, "  return []*ExprGroup{r.Left, r.Right}\n")
	fmt.Fprintf(g.w, "}\n\n")
}

func (g *MemoGen) genChildlessGroupInterface(define ExprDef) {
	fmt.Fprintf(g.w, "func (r *%s) Children() []*ExprGroup {\n", define.Name)
	fmt.Fprintf(g.w, "  return nil\n")
	fmt.Fprintf(g.w, "}\n\n")
}

func (g *MemoGen) genUnaryGroupInterface(define ExprDef) {
	fmt.Fprintf(g.w, "func (r *%s) Children() []*ExprGroup {\n", define.Name)
	fmt.Fprintf(g.w, "  return []*ExprGroup{r.Child}\n")
	fmt.Fprintf(g.w, "}\n\n")

	fmt.Fprintf(g.w, "func (r *%s) outputCols() sql.ColSet {\n", define.Name)
	switch define.Name {
	case "Project":
		fmt.Fprintf(g.w, "  return getProjectColset(r)\n")

	default:
		fmt.Fprintf(g.w, "  return r.Child.RelProps.OutputCols()\n")
	}

	fmt.Fprintf(g.w, "}\n\n")

}

func (g *MemoGen) genFormatters(defines []ExprDef) {
	// printer
	fmt.Fprintf(g.w, "func FormatExpr(r exprType) string {\n")
	fmt.Fprintf(g.w, "  switch r := r.(type) {\n")
	for _, d := range defines {
		loweredName := strings.ToLower(d.Name)
		fmt.Fprintf(g.w, "  case *%s:\n", d.Name)
		if loweredName == "indexscan" {
			fmt.Fprintf(g.w, "    if r.Alias != \"\" {\n")
			fmt.Fprintf(g.w, "      return fmt.Sprintf(\"%s: %%s\", r.Alias)\n", loweredName)
			fmt.Fprintf(g.w, "    }\n")
		}
		if d.SourceType != "" {
			fmt.Fprintf(g.w, "    return fmt.Sprintf(\"%s: %%s\", r.Name())\n", loweredName)
		} else if d.Join || d.Binary {
			fmt.Fprintf(g.w, "    return fmt.Sprintf(\"%s %%d %%d\", r.Left.Id, r.Right.Id)\n", loweredName)
		} else if d.Unary {
			fmt.Fprintf(g.w, "    return fmt.Sprintf(\"%s: %%d\", r.Child.Id)\n", loweredName)
		} else {
			panic("unreachable")
		}
	}
	fmt.Fprintf(g.w, "  default:\n")
	fmt.Fprintf(g.w, "    panic(fmt.Sprintf(\"unknown RelExpr type: %%T\", r))\n")
	fmt.Fprintf(g.w, "  }\n")
	fmt.Fprintf(g.w, "}\n\n")

	// to sqlNode
	fmt.Fprintf(g.w, "func buildRelExpr(b *ExecBuilder, r RelExpr, children ...sql.Node) (sql.Node, error) {\n")
	fmt.Fprintf(g.w, "  var result sql.Node\n")
	fmt.Fprintf(g.w, "  var err error\n\n")
	fmt.Fprintf(g.w, "  switch r := r.(type) {\n")
	for _, d := range defines {
		if d.SkipExec {
			continue
		}
		fmt.Fprintf(g.w, "  case *%s:\n", d.Name)
		fmt.Fprintf(g.w, "  result, err = b.build%s(r, children...)\n", strings.Title(d.Name))
	}
	fmt.Fprintf(g.w, "  default:\n")
	fmt.Fprintf(g.w, "    panic(fmt.Sprintf(\"unknown RelExpr type: %%T\", r))\n")
	fmt.Fprintf(g.w, "  }\n\n")
	fmt.Fprintf(g.w, "  if err != nil {\n")
	fmt.Fprintf(g.w, "    return nil, err\n")
	fmt.Fprintf(g.w, "  }\n\n")
	fmt.Fprintf(g.w, "  result, err = r.Group().finalize(result)\n")
	fmt.Fprintf(g.w, "  if err != nil {\n")
	fmt.Fprintf(g.w, "    return nil, err\n")
	fmt.Fprintf(g.w, "  }\n")
	fmt.Fprintf(g.w, "  return result, nil\n")
	fmt.Fprintf(g.w, "}\n\n")
}
