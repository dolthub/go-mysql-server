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
	defns []ExprDef
	w     io.Writer
}

func (g *MemoGen) Generate(defns GenDefs, w io.Writer) {
	g.defns = defns.(MemoExprs).Exprs

	g.w = w

	g.genImport()
	for _, defn := range g.defns {
		g.genType(defn)
		g.genRelInterfaces(defn)

		g.genStringer(defn)
		g.genFormatter(defn)
		if defn.SourceType != "" {
			g.genSourceRelInterface(defn)
		}
		if defn.Join {
			g.genJoinRelInterface(defn)
		} else if defn.Binary {
			g.genBinaryGroupInterface(defn)
		} else if defn.Unary {
			g.genUnaryGroupInterface(defn)
		} else {
			g.genChildlessGroupInterface(defn)
		}
	}
	g.genBuildRelExpr(g.defns)
}

func (g *MemoGen) genImport() {
	fmt.Fprintf(g.w, "import (\n")
	fmt.Fprintf(g.w, "  \"fmt\"\n")
	fmt.Fprintf(g.w, "  \"strings\"\n")
	fmt.Fprintf(g.w, "  \"github.com/dolthub/go-mysql-server/sql\"\n")
	fmt.Fprintf(g.w, "  \"github.com/dolthub/go-mysql-server/sql/plan\"\n")
	fmt.Fprintf(g.w, ")\n\n")
}

func (g *MemoGen) genType(defn ExprDef) {
	fmt.Fprintf(g.w, "type %s struct {\n", strings.Title(defn.Name))
	if defn.SourceType != "" {
		fmt.Fprintf(g.w, "  *sourceBase\n")
		fmt.Fprintf(g.w, "  Table %s\n", defn.SourceType)
	} else if defn.Join {
		fmt.Fprintf(g.w, "  *JoinBase\n")
	} else if defn.Unary {
		fmt.Fprintf(g.w, "  *relBase\n")
		fmt.Fprintf(g.w, "  Child *ExprGroup\n")
	} else if defn.Binary {
		fmt.Fprintf(g.w, "  *relBase\n")
		fmt.Fprintf(g.w, "  Left *ExprGroup\n")
		fmt.Fprintf(g.w, "  Right *ExprGroup\n")
	}
	for _, attr := range defn.Attrs {
		fmt.Fprintf(g.w, "  %s %s\n", strings.Title(attr[0]), attr[1])
	}

	fmt.Fprintf(g.w, "}\n\n")
}

func (g *MemoGen) genRelInterfaces(defn ExprDef) {
	fmt.Fprintf(g.w, "var _ RelExpr = (*%s)(nil)\n", defn.Name)
	fmt.Fprintf(g.w, "var _ fmt.Formatter = (*%s)(nil)\n", defn.Name)
	fmt.Fprintf(g.w, "var _ fmt.Stringer = (*%s)(nil)\n", defn.Name)
	if defn.SourceType != "" {
		fmt.Fprintf(g.w, "var _ SourceRel = (*%s)(nil)\n", defn.Name)
	} else if defn.Join {
		fmt.Fprintf(g.w, "var _ JoinRel = (*%s)(nil)\n", defn.Name)
	} else if defn.Unary || defn.Binary {
	} else {
		panic("unreachable")
	}
	fmt.Fprintf(g.w, "\n")
}

func (g *MemoGen) genScalarInterfaces(defn ExprDef) {
	fmt.Fprintf(g.w, "var _ ScalarExpr = (*%s)(nil)\n", defn.Name)

	fmt.Fprintf(g.w, "\n")

	fmt.Fprintf(g.w, "func (r *%s) ExprId() ScalarExprId {\n", defn.Name)
	fmt.Fprintf(g.w, "  return ScalarExpr%s\n", strings.Title(defn.Name))
	fmt.Fprintf(g.w, "}\n\n")
}

func (g *MemoGen) genStringer(defn ExprDef) {
	fmt.Fprintf(g.w, "func (r *%s) String() string {\n", defn.Name)
	fmt.Fprintf(g.w, "  return fmt.Sprintf(\"%%s\", r)\n")
	fmt.Fprintf(g.w, "}\n\n")
}

func (g *MemoGen) genFormatter(defn ExprDef) {
	fmt.Fprintf(g.w, "func (r *%s) Format(s fmt.State, verb rune) {\n", defn.Name)
	fmt.Fprintf(g.w, "  FormatExpr(r, s, verb)\n")
	fmt.Fprintf(g.w, "}\n\n")
}

func (g *MemoGen) genSourceRelInterface(defn ExprDef) {
	fmt.Fprintf(g.w, "func (r *%s) Name() string {\n", defn.Name)
	if !defn.SkipName {
		fmt.Fprintf(g.w, "  return strings.ToLower(r.Table.Name())\n")
	} else {
		fmt.Fprintf(g.w, "  return \"\"\n")
	}
	fmt.Fprintf(g.w, "}\n\n")

	fmt.Fprintf(g.w, "func (r *%s) TableId() sql.TableId {\n", defn.Name)
	fmt.Fprintf(g.w, "  return TableIdForSource(r.g.Id)\n")
	fmt.Fprintf(g.w, "}\n\n")

	fmt.Fprintf(g.w, "func (r *%s) TableIdNode() plan.TableIdNode {\n", defn.Name)
	if defn.SkipTableId {
		fmt.Fprintf(g.w, "  return nil\n")
	} else {
		fmt.Fprintf(g.w, "  return r.Table\n")
	}
	fmt.Fprintf(g.w, "}\n\n")

	fmt.Fprintf(g.w, "func (r *%s) OutputCols() sql.Schema {\n", defn.Name)
	fmt.Fprintf(g.w, "  return r.Table.Schema()\n")
	fmt.Fprintf(g.w, "}\n\n")
}

func (g *MemoGen) genJoinRelInterface(defn ExprDef) {
	fmt.Fprintf(g.w, "func (r *%s) JoinPrivate() *JoinBase {\n", defn.Name)
	fmt.Fprintf(g.w, "  return r.JoinBase\n")
	fmt.Fprintf(g.w, "}\n\n")
}

func (g *MemoGen) genBinaryGroupInterface(defn ExprDef) {
	fmt.Fprintf(g.w, "func (r *%s) Children() []*ExprGroup {\n", defn.Name)
	fmt.Fprintf(g.w, "  return []*ExprGroup{r.Left, r.Right}\n")
	fmt.Fprintf(g.w, "}\n\n")
}

func (g *MemoGen) genChildlessGroupInterface(defn ExprDef) {
	fmt.Fprintf(g.w, "func (r *%s) Children() []*ExprGroup {\n", defn.Name)
	fmt.Fprintf(g.w, "  return nil\n")
	fmt.Fprintf(g.w, "}\n\n")
}

func (g *MemoGen) genUnaryGroupInterface(defn ExprDef) {
	fmt.Fprintf(g.w, "func (r *%s) Children() []*ExprGroup {\n", defn.Name)
	fmt.Fprintf(g.w, "  return []*ExprGroup{r.Child}\n")
	fmt.Fprintf(g.w, "}\n\n")

	fmt.Fprintf(g.w, "func (r *%s) outputCols() sql.ColSet {\n", defn.Name)
	switch defn.Name {
	case "Project":
		fmt.Fprintf(g.w, "  return getProjectColset(r)\n")

	default:
		fmt.Fprintf(g.w, "  return r.Child.RelProps.OutputCols()\n")
	}

	fmt.Fprintf(g.w, "}\n\n")
}

func (g *MemoGen) genBuildRelExpr(defns []ExprDef) {
	fmt.Fprintf(g.w, "func buildRelExpr(b *ExecBuilder, r RelExpr, children ...sql.Node) (sql.Node, error) {\n")
	fmt.Fprintf(g.w, "  var result sql.Node\n")
	fmt.Fprintf(g.w, "  var err error\n\n")
	fmt.Fprintf(g.w, "  switch r := r.(type) {\n")
	for _, d := range defns {
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
	fmt.Fprintf(g.w, "if withDescribeStats, ok := result.(sql.WithDescribeStats); ok {\n")
	fmt.Fprintf(g.w, "	withDescribeStats.SetDescribeStats(*DescribeStats(r))\n")
	fmt.Fprintf(g.w, "}\n")
	fmt.Fprintf(g.w, "  result, err = r.Group().finalize(result)\n")
	fmt.Fprintf(g.w, "  if err != nil {\n")
	fmt.Fprintf(g.w, "    return nil, err\n")
	fmt.Fprintf(g.w, "  }\n")
	fmt.Fprintf(g.w, "  return result, nil\n")
	fmt.Fprintf(g.w, "}\n\n")
}
