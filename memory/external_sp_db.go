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

package memory

import (
	"fmt"
	"math"
	"reflect"
	"strings"
	"time"

	"github.com/shopspring/decimal"

	"github.com/dolthub/go-mysql-server/sql"
)

var (
	externalSPSchemaInt = sql.Schema{&sql.Column{
		Name: "a",
		Type: sql.Int64,
	}}
	externalSPSchemaText = sql.Schema{&sql.Column{
		Name: "a",
		Type: sql.LongText,
	}}
	externalStoredProcedures = []sql.ExternalStoredProcedureDetails{
		{
			Name:     "memory_inout_add",
			Schema:   nil,
			Function: inout_add,
		},
		{
			Name:     "memory_overloaded_mult",
			Schema:   externalSPSchemaInt,
			Function: overloaded_mult1,
		},
		{
			Name:     "memory_overloaded_mult",
			Schema:   externalSPSchemaInt,
			Function: overloaded_mult2,
		},
		{
			Name:     "memory_overloaded_mult",
			Schema:   externalSPSchemaInt,
			Function: overloaded_mult3,
		},
		{
			Name:     "memory_overloaded_type_test",
			Schema:   externalSPSchemaInt,
			Function: overloaded_type_test1,
		},
		{
			Name:     "memory_overloaded_type_test",
			Schema:   externalSPSchemaText,
			Function: overloaded_type_test2,
		},
		{
			Name:     "memory_inout_bool_byte",
			Schema:   nil,
			Function: inout_bool_byte,
		},
		{
			Name:     "memory_error_table_not_found",
			Schema:   nil,
			Function: error_table_not_found,
		},
		{
			Name:     "memory_variadic_add",
			Schema:   externalSPSchemaInt,
			Function: variadic_add,
		},
		{
			Name:     "memory_variadic_byte_slice",
			Schema:   externalSPSchemaText,
			Function: variadic_byte_slice,
		},
		{
			Name:     "memory_variadic_overload",
			Schema:   externalSPSchemaText,
			Function: variadic_overload1,
		},
		{
			Name:     "memory_variadic_overload",
			Schema:   externalSPSchemaText,
			Function: variadic_overload2,
		},
	}
)

// ExternalStoredProcedureProvider is an implementation of sql.ExternalStoredProcedureProvider for the memory db.
type ExternalStoredProcedureProvider struct {
	procedureDirectory ExternalStoredProcedureDirectory
}

var _ sql.ExternalStoredProcedureProvider = (*ExternalStoredProcedureProvider)(nil)

// NewExternalStoredProcedureProvider returns a new ExternalStoredProcedureProvider.
func NewExternalStoredProcedureProvider() ExternalStoredProcedureProvider {
	procedureDirectory := NewExternalStoredProcedureDirectory()
	for _, esp := range externalStoredProcedures {
		procedureDirectory.Register(esp)
	}

	return ExternalStoredProcedureProvider{
		procedureDirectory: procedureDirectory,
	}
}

// ExternalStoredProcedure implements the sql.ExternalStoredProcedureProvider interface
func (e ExternalStoredProcedureProvider) ExternalStoredProcedure(_ *sql.Context, name string, numOfParams int) (*sql.ExternalStoredProcedureDetails, error) {
	return e.procedureDirectory.LookupByNameAndParamCount(name, numOfParams)
}

// ExternalStoredProcedures implements the sql.ExternalStoredProcedureProvider interface
func (e ExternalStoredProcedureProvider) ExternalStoredProcedures(_ *sql.Context, name string) ([]sql.ExternalStoredProcedureDetails, error) {
	return e.procedureDirectory.LookupByName(name)
}

func inout_add(_ *sql.Context, a *int64, b int64) (sql.RowIter, error) {
	*a = *a + b
	return sql.RowsToRowIter(), nil
}

func overloaded_mult1(_ *sql.Context, a int8) (sql.RowIter, error) {
	return sql.RowsToRowIter(sql.Row{int64(a)}), nil
}
func overloaded_mult2(_ *sql.Context, a int16, b int32) (sql.RowIter, error) {
	return sql.RowsToRowIter(sql.Row{int64(a) * int64(b)}), nil
}
func overloaded_mult3(_ *sql.Context, a int8, b int32, c int64) (sql.RowIter, error) {
	return sql.RowsToRowIter(sql.Row{int64(a) * int64(b) * c}), nil
}

func overloaded_type_test1(
	_ *sql.Context,
	aa int8, ab int16, ac int, ad int32, ae int64, af float32, ag float64,
	ba *int8, bb *int16, bc *int, bd *int32, be *int64, bf *float32, bg *float64,
) (sql.RowIter, error) {
	return sql.RowsToRowIter(sql.Row{
		int64(aa) + int64(ab) + int64(ac) + int64(ad) + int64(ae) + int64(af) + int64(ag) +
			int64(*ba) + int64(*bb) + int64(*bc) + int64(*bd) + int64(*be) + int64(*bf) + int64(*bg),
	}), nil
}
func overloaded_type_test2(
	_ *sql.Context,
	aa bool, ab string, ac []byte, ad time.Time, ae decimal.Decimal,
	ba *bool, bb *string, bc *[]byte, bd *time.Time, be *decimal.Decimal,
) (sql.RowIter, error) {
	return sql.RowsToRowIter(sql.Row{
		fmt.Sprintf(`aa:%v,ba:%v,ab:"%s",bb:"%s",ac:%v,bc:%v,ad:%s,bd:%s,ae:%s,be:%s`,
			aa, *ba, ab, *bb, ac, *bc, ad.Format("2006-01-02"), (*bd).Format("2006-01-02"), ae.String(), (*be).String()),
	}), nil
}

func inout_bool_byte(_ *sql.Context, a bool, b *bool, c []byte, d *[]byte) (sql.RowIter, error) {
	a = !a
	*b = !*b
	for i := range c {
		c[i] = c[i] + 1
	}
	for i := range *d {
		(*d)[i] = (*d)[i] + 1
	}
	return nil, nil
}

func error_table_not_found(_ *sql.Context) (sql.RowIter, error) {
	return nil, sql.ErrTableNotFound.New("non_existent_table")
}

func variadic_add(_ *sql.Context, vals ...int) (sql.RowIter, error) {
	sum := int64(0)
	for _, val := range vals {
		sum += int64(val)
	}
	return sql.RowsToRowIter(sql.Row{sum}), nil
}

func variadic_byte_slice(_ *sql.Context, vals ...[]byte) (sql.RowIter, error) {
	sb := strings.Builder{}
	for _, val := range vals {
		sb.Write(val)
	}
	return sql.RowsToRowIter(sql.Row{sb.String()}), nil
}

func variadic_overload1(_ *sql.Context, a string, b string) (sql.RowIter, error) {
	return sql.RowsToRowIter(sql.Row{fmt.Sprintf("%s-%s", a, b)}), nil
}

func variadic_overload2(_ *sql.Context, a string, b string, vals ...uint8) (sql.RowIter, error) {
	return sql.RowsToRowIter(sql.Row{fmt.Sprintf("%s,%s,%v", a, b, vals)}), nil
}

// ExternalStoredProcedureDirectory manages a collection of ExternalStoredProcedures and encapsulates
// the logic for looking up external stored procedures based on name and number of arguments.
type ExternalStoredProcedureDirectory struct {
	procedures map[string]map[int]sql.ExternalStoredProcedureDetails
}

// NewExternalStoredProcedureDirectory creates a new, empty instance of ExternalStoredProcedureDirectory.
func NewExternalStoredProcedureDirectory() ExternalStoredProcedureDirectory {
	return ExternalStoredProcedureDirectory{
		procedures: make(map[string]map[int]sql.ExternalStoredProcedureDetails),
	}
}

// Register adds an external stored procedure to this directory.
func (epd *ExternalStoredProcedureDirectory) Register(procedureDetails sql.ExternalStoredProcedureDetails) {
	numOfParams := epd.countNumberOfParams(procedureDetails)

	if _, ok := epd.procedures[procedureDetails.Name]; !ok {
		epd.procedures[procedureDetails.Name] = make(map[int]sql.ExternalStoredProcedureDetails)
	}
	epd.procedures[procedureDetails.Name][numOfParams] = procedureDetails
}

// LookupByName returns all stored procedure variants registered with the specified name, no matter
// how many parameters they require. If no external stored procedures are registered with the specified
// name, an ErrStoredProcedureDoesNotExist error is returned.
func (epd *ExternalStoredProcedureDirectory) LookupByName(name string) ([]sql.ExternalStoredProcedureDetails, error) {
	procedureVariants, ok := epd.procedures[strings.ToLower(name)]
	if !ok {
		return nil, sql.ErrStoredProcedureDoesNotExist.New(name)
	}

	procedures := make([]sql.ExternalStoredProcedureDetails, 0, len(procedureVariants))
	for _, procedure := range procedureVariants {
		procedures = append(procedures, procedure)
	}
	return procedures, nil
}

// LookupByNameAndParamCount returns the external stored procedure registered with the specified name
// and able to accept the specified number of parameters. If no external stored procedures are
// registered with the specified name and able to accept the specified number of parameters, an
// ErrStoredProcedureDoesNotExist error is returned.
func (epd *ExternalStoredProcedureDirectory) LookupByNameAndParamCount(name string, numOfParams int) (*sql.ExternalStoredProcedureDetails, error) {
	procedureVariants, ok := epd.procedures[strings.ToLower(name)]
	if !ok {
		return nil, sql.ErrStoredProcedureDoesNotExist.New(name)
	}

	// If we find an exact match on param count, return that stored procedure
	procedure, ok := procedureVariants[numOfParams]
	if ok {
		return &procedure, nil
	}

	// Otherwise, find the largest param length and return that stored procedure
	var largestParamLen int
	var largestParamProc *sql.ExternalStoredProcedureDetails
	for paramLen, procedure := range procedureVariants {
		if largestParamProc == nil || largestParamLen < paramLen {
			largestParamProc = &procedure
			largestParamLen = paramLen
		}
	}
	return largestParamProc, nil
}

// countNumberOfParams returns the number of parameters accepted by the specified external stored
// procedure, including allowing variadic return types to expand to accept at most MaxInt parameters.
func (epd *ExternalStoredProcedureDirectory) countNumberOfParams(externalProcedure sql.ExternalStoredProcedureDetails) int {
	funcVal := reflect.ValueOf(externalProcedure.Function)
	funcType := funcVal.Type()

	// Return MaxInt for variadic types, since they can accommodate any number of params
	if funcVal.Type().IsVariadic() {
		return math.MaxInt
	}

	// We subtract one because ctx is required to always be the first parameter to a function, but
	// customers won't actually pass that in to the stored procedure.
	// TODO: Should we keep the ctx check in here?
	return funcType.NumIn() - 1
}
