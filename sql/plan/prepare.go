// Copyright 2022 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	"fmt"
	"strings"

	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/transform"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
)

// PrepareDelayedParserFunc is the parse function from the parse package. We accept the function to get around import
// cycles.
type PrepareDelayedParserFunc func(*sql.Context) (sql.Node, error)

// PrepareDelayedAnalyzerFunc is one of the analyzer functions from the analyzer package. We accept the function to get
// around import cycles.
type PrepareDelayedAnalyzerFunc func(*sql.Context, sql.Node) (sql.Node, error)

// PrepareQuery is a node that prepares the query
type PrepareQuery struct {
	Name            string
	DelayedParser   PrepareDelayedParserFunc
	DelayedAnalyzer PrepareDelayedAnalyzerFunc
	Cache           *sql.PreparedDataCache

	delayedOpChecker sql.PrivilegedOperationChecker
}

var _ sql.Node = (*PrepareQuery)(nil)
var _ sql.PreparedDataCacher = (*PrepareQuery)(nil)
var _ sql.OpaqueNode = (*PrepareQuery)(nil)

// NewPrepareQuery creates a new PrepareQuery node.
func NewPrepareQuery(name string, parser PrepareDelayedParserFunc) *PrepareQuery {
	return &PrepareQuery{
		Name:          name,
		DelayedParser: parser,
	}
}

// WithDelayedAnalyzer allows PREPARE to analyze the given string at runtime rather than during the analysis period.
func (p *PrepareQuery) WithDelayedAnalyzer(analyzer PrepareDelayedAnalyzerFunc) *PrepareQuery {
	np := *p
	np.DelayedAnalyzer = analyzer
	return &np
}

// Schema implements the Node interface.
func (p *PrepareQuery) Schema() sql.Schema {
	return types.OkResultSchema
}

// PrepareInfo is the Info for OKResults returned by Update nodes.
type PrepareInfo struct {
}

// String implements fmt.Stringer
func (pi PrepareInfo) String() string {
	return "Statement prepared"
}

// RowIter implements the Node interface.
func (p *PrepareQuery) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	parsedChild, err := p.DelayedParser(ctx)
	if err != nil {
		return nil, err
	}
	analyzedChild, err := p.DelayedAnalyzer(ctx, parsedChild)
	if err != nil {
		return nil, err
	}
	if !analyzedChild.CheckPrivileges(ctx, p.delayedOpChecker) {
		return nil, sql.ErrPrivilegeCheckFailed.New(p.formatUserString(ctx))
	}
	p.Cache.CacheStmt(ctx.Session.ID(), p.Name, analyzedChild)
	return sql.RowsToRowIter(sql.NewRow(types.OkResult{RowsAffected: 0, Info: PrepareInfo{}})), nil
}

func (p *PrepareQuery) Resolved() bool {
	return true
}

// Children implements the Node interface.
func (p *PrepareQuery) Children() []sql.Node {
	return nil
}

// WithChildren implements the Node interface.
func (p *PrepareQuery) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) > 0 {
		return nil, sql.ErrInvalidChildrenNumber.New(p, len(children), 0)
	}
	return p, nil
}

// CheckPrivileges implements the interface sql.Node.
func (p *PrepareQuery) CheckPrivileges(ctx *sql.Context, opChecker sql.PrivilegedOperationChecker) bool {
	p.delayedOpChecker = opChecker
	return true
}

func (p *PrepareQuery) String() string {
	return "Prepare"
}

// WithPreparedDataCache implements the interface sql.PreparedDataCacher.
func (p *PrepareQuery) WithPreparedDataCache(pdc *sql.PreparedDataCache) (sql.Node, error) {
	np := *p
	np.Cache = pdc
	return &np, nil
}

// Opaque implements the interface sql.OpaqueNode.
func (p *PrepareQuery) Opaque() bool {
	return true
}

// formatUserString is a helper for formatting the user string, since prepared statements delay the privilege checker.
func (p PrepareQuery) formatUserString(ctx *sql.Context) string {
	client := ctx.Client()
	user := strings.ReplaceAll(client.User, "'", "''")
	host := strings.ReplaceAll(client.Address, "'", "''")
	return fmt.Sprintf("'%s'@'%s'", user, host)
}

// ExecuteQuery is a node that prepares the query
type ExecuteQuery struct {
	Name            string
	BindVars        []sql.Expression
	Cache           *sql.PreparedDataCache
	DelayedAnalyzer PrepareDelayedAnalyzerFunc

	analyzedNode     sql.Node
	delayedOpChecker sql.PrivilegedOperationChecker
}

var _ sql.Node = (*ExecuteQuery)(nil)
var _ sql.PreparedDataCacher = (*ExecuteQuery)(nil)

// NewExecuteQuery executes a prepared statement
func NewExecuteQuery(name string, bindVars ...sql.Expression) *ExecuteQuery {
	return &ExecuteQuery{Name: name, BindVars: bindVars}
}

// WithDelayedAnalyzer allows EXECUTE to analyze the cached node at runtime rather than during the analysis period.
func (p *ExecuteQuery) WithDelayedAnalyzer(analyzer PrepareDelayedAnalyzerFunc) *ExecuteQuery {
	np := *p
	np.DelayedAnalyzer = analyzer
	return &np
}

// Schema implements the Node interface.
func (p *ExecuteQuery) Schema() sql.Schema {
	if p.analyzedNode != nil {
		return p.analyzedNode.Schema()
	}
	return nil
}

// RowIter implements the Node interface.
func (p *ExecuteQuery) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	cachedStatement, ok := p.Cache.GetCachedStmt(ctx.Session.ID(), p.Name)
	if !ok {
		return nil, sql.ErrUnknownPreparedStatement.New(p.Name)
	}

	// number of BindVars provided must match number of BindVars expected
	if p.countBindVars(cachedStatement) != len(p.BindVars) {
		return nil, sql.ErrInvalidArgument.New(p.Name)
	}

	bindings := map[string]sql.Expression{}
	for i, binding := range p.BindVars {
		varName := fmt.Sprintf("v%d", i+1)
		bindings[varName] = binding
	}

	if len(bindings) > 0 {
		var usedBindings map[string]bool
		var err error
		cachedStatement, usedBindings, err = ApplyBindings(cachedStatement, bindings)
		if err != nil {
			return nil, err
		}
		for binding := range bindings {
			if !usedBindings[binding] {
				return nil, fmt.Errorf("unused binding %s", binding)
			}
		}
	}

	analyzed, err := p.DelayedAnalyzer(ctx, cachedStatement)
	if err != nil {
		return nil, err
	}
	if !analyzed.CheckPrivileges(ctx, p.delayedOpChecker) {
		return nil, sql.ErrPrivilegeCheckFailed.New(PrepareQuery{}.formatUserString(ctx))
	}
	p.analyzedNode = analyzed
	return analyzed.RowIter(ctx, row)
}

func (p *ExecuteQuery) Resolved() bool {
	return true
}

// Children implements the Node interface.
func (p *ExecuteQuery) Children() []sql.Node {
	return nil
}

// WithChildren implements the Node interface.
func (p *ExecuteQuery) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) > 0 {
		return nil, sql.ErrInvalidChildrenNumber.New(p, len(children), 0)
	}
	return p, nil
}

// CheckPrivileges implements the interface sql.Node.
func (p *ExecuteQuery) CheckPrivileges(ctx *sql.Context, opChecker sql.PrivilegedOperationChecker) bool {
	p.delayedOpChecker = opChecker
	return true
}

func (p *ExecuteQuery) String() string {
	return "Execute"
}

// WithPreparedDataCache implements the interface sql.PreparedDataCacher.
func (p *ExecuteQuery) WithPreparedDataCache(pdc *sql.PreparedDataCache) (sql.Node, error) {
	np := *p
	np.Cache = pdc
	return &np, nil
}

// Opaque implements the interface sql.OpaqueNode.
func (p *ExecuteQuery) Opaque() bool {
	return true
}

// Count number of BindVars in given tree
func (p *ExecuteQuery) countBindVars(node sql.Node) int {
	bindCnt := 0
	bindCntFunc := func(e sql.Expression) bool {
		if _, ok := e.(*expression.BindVar); ok {
			bindCnt++
		}
		return true
	}
	transform.InspectExpressions(node, bindCntFunc)

	// InsertInto.Source not a child of InsertInto, so also need to traverse those
	transform.Inspect(node, func(n sql.Node) bool {
		if in, ok := n.(*InsertInto); ok {
			transform.InspectExpressions(in.Source, bindCntFunc)
			return false
		}
		return true
	})
	return bindCnt
}

// DeallocateQuery is a node that deallocates the prepared query
type DeallocateQuery struct {
	Name  string
	Cache *sql.PreparedDataCache
}

var _ sql.Node = (*DeallocateQuery)(nil)
var _ sql.PreparedDataCacher = (*DeallocateQuery)(nil)

// NewDeallocateQuery executes a prepared statement
func NewDeallocateQuery(name string) *DeallocateQuery {
	return &DeallocateQuery{Name: name}
}

// Schema implements the Node interface.
func (p *DeallocateQuery) Schema() sql.Schema {
	return types.OkResultSchema
}

// RowIter implements the Node interface.
func (p *DeallocateQuery) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	if _, ok := p.Cache.GetCachedStmt(ctx.Session.ID(), p.Name); !ok {
		return nil, sql.ErrUnknownPreparedStatement.New(p.Name)
	}
	p.Cache.UncacheStmt(ctx.Session.ID(), p.Name)
	return sql.RowsToRowIter(sql.NewRow(types.OkResult{})), nil
}

func (p *DeallocateQuery) Resolved() bool {
	return true
}

// Children implements the Node interface.
func (p *DeallocateQuery) Children() []sql.Node {
	return nil
}

// WithChildren implements the Node interface.
func (p *DeallocateQuery) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) > 0 {
		return nil, sql.ErrInvalidChildrenNumber.New(p, len(children), 0)
	}
	return p, nil
}

// CheckPrivileges implements the interface sql.Node.
func (p *DeallocateQuery) CheckPrivileges(ctx *sql.Context, opChecker sql.PrivilegedOperationChecker) bool {
	return true
}

func (p *DeallocateQuery) String() string {
	return fmt.Sprintf("Deallocate(%s)", p.Name)
}

// WithPreparedDataCache implements the interface sql.PreparedDataCacher.
func (p *DeallocateQuery) WithPreparedDataCache(pdc *sql.PreparedDataCache) (sql.Node, error) {
	np := *p
	np.Cache = pdc
	return &np, nil
}
