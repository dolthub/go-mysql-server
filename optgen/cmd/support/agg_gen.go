package support

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"strings"

	"gopkg.in/yaml.v3"
)

type AggDefs struct {
	UnaryAggs []AggDef `yaml:"unaryAggs"`
}

type AggDef struct {
	Name     string `yaml:"name"`
	SqlName  string `yaml:"sqlName"`
	Desc     string `yaml:"desc"`
	RetType  string `yaml:"retType"` // must be valid sql.Type
	Nullable bool   `yaml:"nullable"`
}

var _ GenDefs = ([]AggDef)(nil)

type AggGen struct {
	defines []AggDef
	w       io.Writer
}

func DecodeUnaryAggDefs(path string) (AggDefs, error) {
	contents, err := os.ReadFile(path)
	if err != nil {
		return AggDefs{}, err
	}
	dec := yaml.NewDecoder(bytes.NewReader(contents))
	dec.KnownFields(true)
	var res AggDefs
	return res, dec.Decode(&res)
}

func (g *AggGen) Generate(defines GenDefs, w io.Writer) {
	g.defines = defines.(AggDefs).UnaryAggs

	g.w = w

	fmt.Fprintf(g.w, "import (\n")
	fmt.Fprintf(g.w, "    \"fmt\"\n")
	fmt.Fprintf(g.w, "    \"github.com/dolthub/go-mysql-server/sql/types\"\n")
	fmt.Fprintf(g.w, "    \"github.com/dolthub/go-mysql-server/sql\"\n")
	fmt.Fprintf(g.w, "    \"github.com/dolthub/go-mysql-server/sql/expression\"\n")
	fmt.Fprintf(g.w, "    \"github.com/dolthub/go-mysql-server/sql/transform\"\n")
	fmt.Fprintf(g.w, ")\n\n")

	for _, define := range g.defines {
		g.genAggType(define)
		g.genAggInterfaces(define)
		g.genAggConstructor(define)
		g.genAggPropAccessors(define)
		g.genAggStringer(define)
		g.genAggWithWindow(define)
		g.genAggWithChildren(define)
		g.genAggNewBuffer(define)
		g.genAggWindowConstructor(define)
	}
}

func (g *AggGen) genAggType(define AggDef) {
	fmt.Fprintf(g.w, "type %s struct{\n", define.Name)
	fmt.Fprintf(g.w, "    unaryAggBase\n")
	fmt.Fprintf(g.w, "}\n\n")
}

func (g *AggGen) genAggInterfaces(define AggDef) {
	fmt.Fprintf(g.w, "var _ sql.FunctionExpression = (*%s)(nil)\n", define.Name)
	fmt.Fprintf(g.w, "var _ sql.Aggregation = (*%s)(nil)\n", define.Name)
	fmt.Fprintf(g.w, "var _ sql.WindowAdaptableExpression = (*%s)(nil)\n", define.Name)
	fmt.Fprintf(g.w, "\n")

}

func (g *AggGen) genAggConstructor(define AggDef) {
	fmt.Fprintf(g.w, "func New%s(e sql.Expression) *%s {\n", define.Name, define.Name)
	fmt.Fprintf(g.w, "    return &%s{\n", define.Name)
	fmt.Fprintf(g.w, "        unaryAggBase{\n")
	fmt.Fprintf(g.w, "            UnaryExpression: expression.UnaryExpression{Child: e},\n")
	fmt.Fprintf(g.w, "            functionName: \"%s\",\n", define.Name)
	fmt.Fprintf(g.w, "            description: \"%s\",\n", define.Desc)
	fmt.Fprintf(g.w, "        },\n")
	fmt.Fprintf(g.w, "    }\n")
	fmt.Fprintf(g.w, "}\n\n")
}

func (g *AggGen) genAggPropAccessors(define AggDef) {
	retType := "a.Child.Type()"
	if define.RetType != "" {
		retType = define.RetType
	}
	fmt.Fprintf(g.w, "func (a *%s) Type() sql.Type {\n", define.Name)
	fmt.Fprintf(g.w, "    return %s\n", retType)
	fmt.Fprintf(g.w, "}\n\n")

	fmt.Fprintf(g.w, "func (a *%s) IsNullable() bool {\n", define.Name)
	fmt.Fprintf(g.w, "    return %t\n", define.Nullable)
	fmt.Fprintf(g.w, "}\n\n")
}

func (g *AggGen) genAggStringer(define AggDef) {
	sqlName := define.Name
	if define.SqlName != "" {
		sqlName = define.SqlName
	}
	fmt.Fprintf(g.w, "func (a *%s) String() string {\n", define.Name)
	fmt.Fprintf(g.w, "  if a.window != nil {\n")
	fmt.Fprintf(g.w, "    pr := sql.NewTreePrinter()\n")
	fmt.Fprintf(g.w, "    _ = pr.WriteNode(\"%s\")\n	", strings.ToUpper(sqlName))
	fmt.Fprintf(g.w, "    children := []string{a.window.String(), a.Child.String()}\n")
	fmt.Fprintf(g.w, "    pr.WriteChildren(children...)\n")
	fmt.Fprintf(g.w, "    return pr.String()\n")
	fmt.Fprintf(g.w, "  }\n")
	fmt.Fprintf(g.w, "  return fmt.Sprintf(\"%s(%%s)\", a.Child)\n", strings.ToUpper(sqlName))
	fmt.Fprintf(g.w, "}\n\n")

	fmt.Fprintf(g.w, "func (a *%s) DebugString() string {\n", define.Name)
	fmt.Fprintf(g.w, "  if a.window != nil {\n")
	fmt.Fprintf(g.w, "    pr := sql.NewTreePrinter()\n")
	fmt.Fprintf(g.w, "    _ = pr.WriteNode(\"%s\")\n	", strings.ToUpper(sqlName))
	fmt.Fprintf(g.w, "    children := []string{sql.DebugString(a.window), sql.DebugString(a.Child)}\n")
	fmt.Fprintf(g.w, "    pr.WriteChildren(children...)\n")
	fmt.Fprintf(g.w, "    return pr.String()\n")
	fmt.Fprintf(g.w, "  }\n")
	fmt.Fprintf(g.w, "  return fmt.Sprintf(\"%s(%%s)\", sql.DebugString(a.Child))\n", strings.ToUpper(sqlName))
	fmt.Fprintf(g.w, "}\n\n")
}

func (g *AggGen) genAggWithChildren(define AggDef) {
	fmt.Fprintf(g.w, "func (a *%s) WithChildren(children ...sql.Expression) (sql.Expression, error) {\n", define.Name)
	fmt.Fprintf(g.w, "    res, err := a.unaryAggBase.WithChildren(children...)\n")
	fmt.Fprintf(g.w, "    return &%s{unaryAggBase: *res.(*unaryAggBase)}, err\n", define.Name)
	fmt.Fprintf(g.w, "}\n\n")
}

func (g *AggGen) genAggWithWindow(define AggDef) {
	fmt.Fprintf(g.w, "func (a *%s) WithWindow(window *sql.WindowDefinition) (sql.Aggregation, error) {\n", define.Name)
	fmt.Fprintf(g.w, "    res, err := a.unaryAggBase.WithWindow(window)\n")
	fmt.Fprintf(g.w, "    return &%s{unaryAggBase: *res.(*unaryAggBase)}, err\n", define.Name)
	fmt.Fprintf(g.w, "}\n\n")
}

func (g *AggGen) genAggWindowConstructor(define AggDef) {
	fmt.Fprintf(g.w, "func (a *%s) NewWindowFunction() (sql.WindowFunction, error) {\n", define.Name)
	fmt.Fprintf(g.w, "    child, err := transform.Clone(a.Child)\n")
	fmt.Fprintf(g.w, "    if err != nil {\n")
	fmt.Fprintf(g.w, "        return nil, err\n")
	fmt.Fprintf(g.w, "    }\n")
	fmt.Fprintf(g.w, "    return New%sAgg(child).WithWindow(a.Window())\n", define.Name)
	fmt.Fprintf(g.w, "}\n\n")
}

func (g *AggGen) genAggNewBuffer(define AggDef) {
	fmt.Fprintf(g.w, "func (a *%s) NewBuffer() (sql.AggregationBuffer, error) {\n", define.Name)
	fmt.Fprintf(g.w, "    child, err := transform.Clone(a.Child)\n")
	fmt.Fprintf(g.w, "    if err != nil {\n")
	fmt.Fprintf(g.w, "        return nil, err\n")
	fmt.Fprintf(g.w, "    }\n")
	fmt.Fprintf(g.w, "    return New%sBuffer(child), nil\n", define.Name)
	fmt.Fprintf(g.w, "}\n\n")
}
