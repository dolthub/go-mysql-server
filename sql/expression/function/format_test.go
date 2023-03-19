// Copyright 2021 Dolthub, Inc.
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

package function

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/src-d/go-errors.v1"

	"github.com/gabereiser/go-mysql-server/sql"
	"github.com/gabereiser/go-mysql-server/sql/expression"
	"github.com/gabereiser/go-mysql-server/sql/types"
)

func TestFormat(t *testing.T) {
	testCases := []struct {
		name     string
		xType    sql.Type
		dType    sql.Type
		row      sql.Row
		expected interface{}
		err      *errors.Kind
	}{
		{"float64 is nil", types.Float64, types.Int32, sql.NewRow(nil, nil, nil), nil, nil},
		{"float64 without d", types.Float64, types.Int32, sql.NewRow(5555.8, nil, nil), nil, nil},
		{"float64 with d", types.Float64, types.Int32, sql.NewRow(5555.855, 4, nil), "5,555.8550", nil},
		{"float64 with super big decimal place", types.Float64, types.Int32, sql.NewRow(5555.855, 15, nil), "5,555.855000000000000", nil},
		{"float64 with negative d", types.Float64, types.Int32, sql.NewRow(5552.855, -1, nil), "5,553", nil},
		{"float64 with float d", types.Float64, types.Float64, sql.NewRow(5555.855, float64(2.123), nil), "5,555.86", nil},
		{"float64 with float negative d", types.Float64, types.Float64, sql.NewRow(5552.855, float64(-1), nil), "5,553", nil},
		{"float64 with blob d", types.Float64, types.Blob, sql.NewRow(5555.855, []byte{1, 2, 3}, nil), "5,555.855000000000500000000000000000", nil},
		{"float64 with text d", types.Float64, types.Text, sql.NewRow(5555.855, "2", nil), "5,555.86", nil},
		{"negative float64 with d", types.Float64, types.Int32, sql.NewRow(-5555.855, 2, nil), "-5,555.86", nil},
		{"blob is nil", types.Blob, types.Int32, sql.NewRow(nil, nil, nil), nil, nil},
		{"blob is ok", types.Blob, types.Int32, sql.NewRow([]byte{1, 2, 3}, nil, nil), nil, nil},
		{"text int without d", types.Text, types.Int32, sql.NewRow("98765432", nil, nil), nil, nil},
		{"text int with d", types.Text, types.Int32, sql.NewRow("98765432", 2, nil), "98,765,432.00", nil},
		{"text int with negative d", types.Text, types.Int32, sql.NewRow("98765432", -1, nil), "98,765,432", nil},
		{"text int with float d", types.Text, types.Float64, sql.NewRow("98765432", 2.123, nil), "98,765,432.00", nil},
		{"text int with float negative d", types.Text, types.Float64, sql.NewRow("98765432", float32(-1), nil), "98,765,432", nil},
		{"text float without d", types.Text, types.Int32, sql.NewRow("98765432.1234", nil, nil), nil, nil},
		{"text float with d", types.Text, types.Int32, sql.NewRow("98765432.1234", 2, nil), "98,765,432.12", nil},
		{"text float with negative d", types.Text, types.Int32, sql.NewRow("98765432.8234", -1, nil), "98,765,433", nil},
		{"text float with float d", types.Text, types.Float64, sql.NewRow("98765432.1234", float64(2.823), nil), "98,765,432.123", nil},
		{"text float with float negative d", types.Text, types.Float64, sql.NewRow("98765432.1234", float64(-1), nil), "98,765,432", nil},
		{"text float with blob d", types.Text, types.Blob, sql.NewRow("98765432.1234", []byte{1, 2, 3}, nil), "98,765,432.123400020000000000000000000000", nil},
		{"negative num text int with d", types.Text, types.Int32, sql.NewRow("-98765432", 2, nil), "-98,765,432.00", nil},
		{"sci-notn big num", types.Float64, types.Int32, sql.NewRow(5932886+.000000000001, 1, nil), "5,932,886.0", nil},
		{"sci-notn big num with big dp", types.Float64, types.Int32, sql.NewRow(5932886+.000000000001, 8, nil), "5,932,886.00000000", nil},
		{"sci-notn big exp num", types.Float64, types.Int32, sql.NewRow(5.932887e+08, 2, nil), "593,288,700.00", nil},
		{"sci-notn neg big exp num", types.Float64, types.Int32, sql.NewRow(-5.932887e+08, 2, nil), "-593,288,700.00", nil},
		{"sci-notn text big exp num", types.Text, types.Int32, sql.NewRow("5.932887e+07", 3, nil), "59,328,870.000", nil},
		{"sci-notn text neg big exp num", types.Text, types.Int32, sql.NewRow("-5.932887e+08", 2, nil), "-593,288,700.00", nil},
		{"sci-notn exp small num", types.Float64, types.Int32, sql.NewRow(5.932887e-08, 2, nil), "0.00", nil},
		{"sci-notn exp small num with big dp", types.Float64, types.Int32, sql.NewRow(5.932887e-08, 9, nil), "0.000000059", nil},
		{"sci-notn neg exp small num", types.Float64, types.Int32, sql.NewRow(-5.932887e-08, 2, nil), "0.00", nil},
		{"sci-notn neg exp small num with big dp", types.Float64, types.Int32, sql.NewRow(-5.932887e-08, 8, nil), "-0.00000006", nil},
		{"sci-notn text neg exp small num", types.Float64, types.Int32, sql.NewRow("-5.932887e-08", 2, nil), "0.00", nil},
		{"sci-notn text neg exp small num with big dp", types.Float64, types.Int32, sql.NewRow("-5.932887e-08", 8, nil), "-0.00000006", nil},
		{"float64 with loc=ar_AE", types.Float64, types.Int32, sql.NewRow(2409384.855, 4, "ar_AE"), "2,409,384.8550", nil},
		{"float64 with loc=ar_BH", types.Float64, types.Int32, sql.NewRow(2409384.855, 4, "ar_BH"), "2,409,384.8550", nil},
		{"float64 with loc=ar_EG", types.Float64, types.Int32, sql.NewRow(2409384.855, 4, "ar_EG"), "2,409,384.8550", nil},
		{"float64 with loc=ar_IQ", types.Float64, types.Int32, sql.NewRow(2409384.855, 4, "ar_IQ"), "2,409,384.8550", nil},
		{"float64 with loc=ar_JO", types.Float64, types.Int32, sql.NewRow(2409384.855, 4, "ar_JO"), "2,409,384.8550", nil},
		{"float64 with loc=ar_KW", types.Float64, types.Int32, sql.NewRow(2409384.855, 4, "ar_KW"), "2,409,384.8550", nil},
		{"float64 with loc=ar_OM", types.Float64, types.Int32, sql.NewRow(2409384.855, 4, "ar_OM"), "2,409,384.8550", nil},
		{"float64 with loc=ar_QA", types.Float64, types.Int32, sql.NewRow(2409384.855, 4, "ar_QA"), "2,409,384.8550", nil},
		{"float64 with loc=ar_SD", types.Float64, types.Int32, sql.NewRow(2409384.855, 4, "ar_SD"), "2,409,384.8550", nil},
		{"float64 with loc=ar_SY", types.Float64, types.Int32, sql.NewRow(2409384.855, 4, "ar_SY"), "2,409,384.8550", nil},
		{"float64 with loc=ar_YE", types.Float64, types.Int32, sql.NewRow(2409384.855, 4, "ar_YE"), "2,409,384.8550", nil},
		{"float64 with loc=da_DK", types.Float64, types.Int32, sql.NewRow(2409384.855, 4, "da_DK"), "2.409.384,8550", nil},
		{"float64 with loc=de_BE", types.Float64, types.Int32, sql.NewRow(2409384.855, 4, "de_BE"), "2.409.384,8550", nil},
		{"float64 with loc=de_DE", types.Float64, types.Int32, sql.NewRow(2409384.855, 4, "de_DE"), "2.409.384,8550", nil},
		{"float64 with loc=de_LU", types.Float64, types.Int32, sql.NewRow(2409384.855, 4, "de_LU"), "2.409.384,8550", nil},
		{"float64 with loc=en_AU", types.Float64, types.Int32, sql.NewRow(2409384.855, 4, "en_AU"), "2,409,384.8550", nil},
		{"float64 with loc=en_CA", types.Float64, types.Int32, sql.NewRow(2409384.855, 4, "en_CA"), "2,409,384.8550", nil},
		{"float64 with loc=en_GB", types.Float64, types.Int32, sql.NewRow(2409384.855, 4, "en_GB"), "2,409,384.8550", nil},
		{"float64 with loc=en_IN", types.Float64, types.Int32, sql.NewRow(2409384.855, 4, "en_IN"), "24,09,384.8550", nil},
		{"float64 with loc=en_NZ", types.Float64, types.Int32, sql.NewRow(2409384.855, 4, "en_NZ"), "2,409,384.8550", nil},
		{"float64 with loc=en_PH", types.Float64, types.Int32, sql.NewRow(2409384.855, 4, "en_PH"), "2,409,384.8550", nil},
		{"float64 with loc=en_US", types.Float64, types.Int32, sql.NewRow(2409384.855, 4, "en_US"), "2,409,384.8550", nil},
		{"float64 with loc=en_ZW", types.Float64, types.Int32, sql.NewRow(2409384.855, 4, "en_ZW"), "2,409,384.8550", nil},
		{"float64 with loc=es_AR", types.Float64, types.Int32, sql.NewRow(2409384.855, 4, "es_AR"), "2.409.384,8550", nil},
		{"float64 with loc=es_US", types.Float64, types.Int32, sql.NewRow(2409384.855, 4, "es_US"), "2,409,384.8550", nil},
		{"float64 with loc=fo_FO", types.Float64, types.Int32, sql.NewRow(2409384.855, 4, "fo_FO"), "2.409.384,8550", nil},
		{"float64 with loc=he_IL", types.Float64, types.Int32, sql.NewRow(2409384.855, 4, "he_IL"), "2,409,384.8550", nil},
		{"float64 with loc=id_ID", types.Float64, types.Int32, sql.NewRow(2409384.855, 4, "id_ID"), "2.409.384,8550", nil},
		{"float64 with loc=is_IS", types.Float64, types.Int32, sql.NewRow(2409384.855, 4, "is_IS"), "2.409.384,8550", nil},
		{"float64 with loc=ja_JP", types.Float64, types.Int32, sql.NewRow(2409384.855, 4, "ja_JP"), "2,409,384.8550", nil},
		{"float64 with loc=ko_KR", types.Float64, types.Int32, sql.NewRow(2409384.855, 4, "ko_KR"), "2,409,384.8550", nil},
		{"float64 with loc=ms_MY", types.Float64, types.Int32, sql.NewRow(2409384.855, 4, "ms_MY"), "2,409,384.8550", nil},
		{"float64 with loc=ro_RO", types.Float64, types.Int32, sql.NewRow(2409384.855, 4, "ro_RO"), "2.409.384,8550", nil},
		{"float64 with loc=ta_IN", types.Float64, types.Int32, sql.NewRow(2409384.855, 4, "ta_IN"), "24,09,384.8550", nil},
		{"float64 with loc=th_TH", types.Float64, types.Int32, sql.NewRow(2409384.855, 4, "th_TH"), "2,409,384.8550", nil},
		{"float64 with loc=tr_TR", types.Float64, types.Int32, sql.NewRow(2409384.855, 4, "tr_TR"), "2.409.384,8550", nil},
		{"float64 with loc=ur_PK", types.Float64, types.Int32, sql.NewRow(2409384.855, 4, "ur_PK"), "2,409,384.8550", nil},
		{"float64 with loc=vi_VN", types.Float64, types.Int32, sql.NewRow(2409384.855, 4, "vi_VN"), "2.409.384,8550", nil},
		{"float64 with loc=zh_CN", types.Float64, types.Int32, sql.NewRow(2409384.855, 4, "zh_CN"), "2,409,384.8550", nil},
		{"float64 with loc=zh_HK", types.Float64, types.Int32, sql.NewRow(2409384.855, 4, "zh_HK"), "2,409,384.8550", nil},
		{"float64 with loc=zh_TW", types.Float64, types.Int32, sql.NewRow(2409384.855, 4, "zh_TW"), "2,409,384.8550", nil},
	}

	for _, tt := range testCases {
		var args = make([]sql.Expression, 3)
		args[0] = expression.NewGetField(0, tt.xType, "Val", false)
		args[1] = expression.NewGetField(1, tt.dType, "Df", false)
		args[2] = expression.NewGetField(2, types.LongText, "Locale", true)
		f, err := NewFormat(args...)

		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)
			require.Nil(err)

			result, err := f.Eval(sql.NewEmptyContext(), tt.row)
			if tt.err != nil {
				require.Error(err)
				require.True(tt.err.Is(err))
			} else {
				require.NoError(err)
				require.Equal(tt.expected, result)
			}
		})
	}
}

// TestSkippedFormat contains all the skipped tests those are incompatible to mysql format function results.
// These include handling text type scientific notation numbers and some locales do not match exactly to mysql format
// results. Some issues include usage of non-ascii characters from language library used and incorrect general format.
// For scietific notation issues:
// FORMAT("5932886+.000000000001", 2) -> Expected: 5932886.00 	|	Actual: value conversion error
// FORMAT(5932886+.000000000001, 15)  -> Expected: "5,932,886.000000000001000"	|	Actual: "5,932,886.000000000000000"
// For some locale issues:
//
//	Expected		| 	Actual
//	2409384,8550	|	2.409.384,8550
//	2,409,384.8550	|	2 409 384,8550
//	2,409,384.8550	|	2.409.384,8550
//	2,409,384.8550	|	٢٬٤٠٩٬٣٨٤¡8550
//	2'409'384.8550	|	2’409’384.8550
func TestSkippedFormat(t *testing.T) {
	testCases := []struct {
		name     string
		xType    sql.Type
		dType    sql.Type
		row      sql.Row
		expected interface{}
		err      *errors.Kind
	}{
		{"sci-notn big num with big dp", types.Float64, types.Int32, sql.NewRow(5932886+.000000000001, 15, nil), "5,932,886.000000000001000", nil},
		{"sci-notn text big num", types.Text, types.Int32, sql.NewRow("5932886+.000000000001", 1, nil), "5,932,886.0", nil},
		{"float64 with loc=ar_DZ", types.Float64, types.Int32, sql.NewRow(2409384.855, 4, "ar_DZ"), "2,409,384.8550", nil},
		{"float64 with loc=ar_IN", types.Float64, types.Int32, sql.NewRow(2409384.855, 4, "ar_IN"), "2,409,384.8550", nil},
		{"float64 with loc=ar_LB", types.Float64, types.Int32, sql.NewRow(2409384.855, 4, "ar_LB"), "2,409,384.8550", nil},
		{"float64 with loc=ar_LY", types.Float64, types.Int32, sql.NewRow(2409384.855, 4, "ar_LY"), "2,409,384.8550", nil},
		{"float64 with loc=ar_MA", types.Float64, types.Int32, sql.NewRow(2409384.855, 4, "ar_MA"), "2,409,384.8550", nil},
		{"float64 with loc=ar_SA", types.Float64, types.Int32, sql.NewRow(2409384.855, 4, "ar_SA"), "2409384.8550", nil},
		{"float64 with loc=ar_TN", types.Float64, types.Int32, sql.NewRow(2409384.855, 4, "ar_TN"), "2,409,384.8550", nil},
		{"float64 with loc=be_BY", types.Float64, types.Int32, sql.NewRow(2409384.855, 4, "be_BY"), "2.409.384,8550", nil},
		{"float64 with loc=bg_BG", types.Float64, types.Int32, sql.NewRow(2409384.855, 4, "bg_BG"), "2409384,8550", nil},
		{"float64 with loc=ca_ES", types.Float64, types.Int32, sql.NewRow(2409384.855, 4, "ca_ES"), "2409384,8550", nil},
		{"float64 with loc=cs_CZ", types.Float64, types.Int32, sql.NewRow(2409384.855, 4, "cs_CZ"), "2409384,8550", nil},
		{"float64 with loc=de_AT", types.Float64, types.Int32, sql.NewRow(2409384.855, 4, "de_AT"), "2409384,8550", nil},
		{"float64 with loc=de_CH", types.Float64, types.Int32, sql.NewRow(2409384.855, 4, "de_CH"), "2'409'384.8550", nil},
		{"float64 with loc=el_GR", types.Float64, types.Int32, sql.NewRow(2409384.855, 4, "el_GR"), "2409384,8550", nil},
		{"float64 with loc=en_ZA", types.Float64, types.Int32, sql.NewRow(2409384.855, 4, "en_ZA"), "2,409,384.8550", nil},
		{"float64 with loc=es_BO", types.Float64, types.Int32, sql.NewRow(2409384.855, 4, "es_BO"), "2409384,8550", nil},
		{"float64 with loc=es_CL", types.Float64, types.Int32, sql.NewRow(2409384.855, 4, "es_CL"), "2409384,8550", nil},
		{"float64 with loc=es_CO", types.Float64, types.Int32, sql.NewRow(2409384.855, 4, "es_CO"), "2409384,8550", nil},
		{"float64 with loc=es_CR", types.Float64, types.Int32, sql.NewRow(2409384.855, 4, "es_CR"), "2409384.8550", nil},
		{"float64 with loc=es_DO", types.Float64, types.Int32, sql.NewRow(2409384.855, 4, "es_DO"), "2409384.8550", nil},
		{"float64 with loc=es_EC", types.Float64, types.Int32, sql.NewRow(2409384.855, 4, "es_EC"), "2409384,8550", nil},
		{"float64 with loc=es_ES", types.Float64, types.Int32, sql.NewRow(2409384.855, 4, "es_ES"), "2409384,8550", nil},
		{"float64 with loc=es_GT", types.Float64, types.Int32, sql.NewRow(2409384.855, 4, "es_GT"), "2409384.8550", nil},
		{"float64 with loc=es_HN", types.Float64, types.Int32, sql.NewRow(2409384.855, 4, "es_HN"), "2409384.8550", nil},
		{"float64 with loc=es_MX", types.Float64, types.Int32, sql.NewRow(2409384.855, 4, "es_MX"), "2409384.8550", nil},
		{"float64 with loc=es_NI", types.Float64, types.Int32, sql.NewRow(2409384.855, 4, "es_NI"), "2409384.8550", nil},
		{"float64 with loc=es_PA", types.Float64, types.Int32, sql.NewRow(2409384.855, 4, "es_PA"), "2409384.8550", nil},
		{"float64 with loc=es_PE", types.Float64, types.Int32, sql.NewRow(2409384.855, 4, "es_PE"), "2409384.8550", nil},
		{"float64 with loc=es_PR", types.Float64, types.Int32, sql.NewRow(2409384.855, 4, "es_PR"), "2409384.8550", nil},
		{"float64 with loc=es_PY", types.Float64, types.Int32, sql.NewRow(2409384.855, 4, "es_PY"), "2409384,8550", nil},
		{"float64 with loc=es_SV", types.Float64, types.Int32, sql.NewRow(2409384.855, 4, "es_SV"), "2409384.8550", nil},
		{"float64 with loc=es_UY", types.Float64, types.Int32, sql.NewRow(2409384.855, 4, "es_UY"), "2409384,8550", nil},
		{"float64 with loc=es_VE", types.Float64, types.Int32, sql.NewRow(2409384.855, 4, "es_VE"), "2409384,8550", nil},
		{"float64 with loc=et_EE", types.Float64, types.Int32, sql.NewRow(2409384.855, 4, "et_EE"), "2 409 384,8550", nil},
		{"float64 with loc=eu_ES", types.Float64, types.Int32, sql.NewRow(2409384.855, 4, "eu_ES"), "2409384,8550", nil},
		{"float64 with loc=fi_FI", types.Float64, types.Int32, sql.NewRow(2409384.855, 4, "fi_FI"), "2 409 384,8550", nil},
		{"float64 with loc=fr_BE", types.Float64, types.Int32, sql.NewRow(2409384.855, 4, "fr_BE"), "2409384,8550", nil},
		{"float64 with loc=fr_CA", types.Float64, types.Int32, sql.NewRow(2409384.855, 4, "fr_CA"), "2409384,8550", nil},
		{"float64 with loc=fr_CH", types.Float64, types.Int32, sql.NewRow(2409384.855, 4, "fr_CH"), "2409384,8550", nil},
		{"float64 with loc=fr_FR", types.Float64, types.Int32, sql.NewRow(2409384.855, 4, "fr_FR"), "2409384,8550", nil},
		{"float64 with loc=fr_LU", types.Float64, types.Int32, sql.NewRow(2409384.855, 4, "fr_LU"), "2409384,8550", nil},
		{"float64 with loc=gl_ES", types.Float64, types.Int32, sql.NewRow(2409384.855, 4, "gl_ES"), "2409384,8550", nil},
		{"float64 with loc=gu_IN", types.Float64, types.Int32, sql.NewRow(2409384.855, 4, "gu_IN"), "2,409,384.8550", nil},
		{"float64 with loc=hi_IN", types.Float64, types.Int32, sql.NewRow(2409384.855, 4, "hi_IN"), "2,409,384.8550", nil},
		{"float64 with loc=hr_HR", types.Float64, types.Int32, sql.NewRow(2409384.855, 4, "hr_HR"), "2409384,8550", nil},
		{"float64 with loc=hu_HU", types.Float64, types.Int32, sql.NewRow(2409384.855, 4, "hu_HU"), "2.409.384,8550", nil},
		{"float64 with loc=it_CH", types.Float64, types.Int32, sql.NewRow(2409384.855, 4, "it_CH"), "2'409'384,8550", nil},
		{"float64 with loc=it_IT", types.Float64, types.Int32, sql.NewRow(2409384.855, 4, "it_IT"), "2409384,8550", nil},
		{"float64 with loc=lt_LT", types.Float64, types.Int32, sql.NewRow(2409384.855, 4, "lt_LT"), "2.409.384,8550", nil},
		{"float64 with loc=lv_LV", types.Float64, types.Int32, sql.NewRow(2409384.855, 4, "lv_LV"), "2 409 384,8550", nil},
		{"float64 with loc=mk_MK", types.Float64, types.Int32, sql.NewRow(2409384.855, 4, "mk_MK"), "2 409 384,8550", nil},
		{"float64 with loc=mn_MN", types.Float64, types.Int32, sql.NewRow(2409384.855, 4, "mn_MN"), "2.409.384,8550", nil},
		{"float64 with loc=nb_NO", types.Float64, types.Int32, sql.NewRow(2409384.855, 4, "nb_NO"), "2.409.384,8550", nil},
		{"float64 with loc=nl_BE", types.Float64, types.Int32, sql.NewRow(2409384.855, 4, "nl_BE"), "2409384,8550", nil},
		{"float64 with loc=nl_NL", types.Float64, types.Int32, sql.NewRow(2409384.855, 4, "nl_NL"), "2409384,8550", nil},
		{"float64 with loc=no_NO", types.Float64, types.Int32, sql.NewRow(2409384.855, 4, "no_NO"), "2.409.384,8550", nil},
		{"float64 with loc=pl_PL", types.Float64, types.Int32, sql.NewRow(2409384.855, 4, "pl_PL"), "2409384,8550", nil},
		{"float64 with loc=pt_BR", types.Float64, types.Int32, sql.NewRow(2409384.855, 4, "pt_BR"), "2409384,8550", nil},
		{"float64 with loc=pt_PT", types.Float64, types.Int32, sql.NewRow(2409384.855, 4, "pt_PT"), "2409384,8550", nil},
		{"float64 with loc=rm_CH", types.Float64, types.Int32, sql.NewRow(2409384.855, 4, "rm_CH"), "2'409'384,8550", nil},
		{"float64 with loc=ru_RU", types.Float64, types.Int32, sql.NewRow(2409384.855, 4, "ru_RU"), "2 409 384,8550", nil},
		{"float64 with loc=ru_UA", types.Float64, types.Int32, sql.NewRow(2409384.855, 4, "ru_UA"), "2.409.384,8550", nil},
		{"float64 with loc=sk_SK", types.Float64, types.Int32, sql.NewRow(2409384.855, 4, "sk_SK"), "2 409 384,8550", nil},
		{"float64 with loc=sl_SI", types.Float64, types.Int32, sql.NewRow(2409384.855, 4, "sl_SI"), "2409384,8550", nil},
		{"float64 with loc=sq_AL", types.Float64, types.Int32, sql.NewRow(2409384.855, 4, "sq_AL"), "2.409.384,8550", nil},
		{"float64 with loc=sr_RS", types.Float64, types.Int32, sql.NewRow(2409384.855, 4, "sr_RS"), "2409384.8550", nil},
		{"float64 with loc=sv_FI", types.Float64, types.Int32, sql.NewRow(2409384.855, 4, "sv_FI"), "2 409 384,8550", nil},
		{"float64 with loc=sv_SE", types.Float64, types.Int32, sql.NewRow(2409384.855, 4, "sv_SE"), "2 409 384,8550", nil},
		{"float64 with loc=te_IN", types.Float64, types.Int32, sql.NewRow(2409384.855, 4, "te_IN"), "24,09,384.8550", nil},
		{"float64 with loc=uk_UA", types.Float64, types.Int32, sql.NewRow(2409384.855, 4, "uk_UA"), "2.409.384,8550", nil},
	}

	for _, tt := range testCases {
		var args = make([]sql.Expression, 3)
		args[0] = expression.NewGetField(0, tt.xType, "Val", false)
		args[1] = expression.NewGetField(1, tt.dType, "Df", false)
		args[2] = expression.NewGetField(2, types.LongText, "Locale", true)
		f, err := NewFormat(args...)

		t.Run(tt.name, func(t *testing.T) {
			t.Skip()
			require := require.New(t)
			require.Nil(err)

			result, err := f.Eval(sql.NewEmptyContext(), tt.row)
			if tt.err != nil {
				require.Error(err)
				require.True(tt.err.Is(err))
			} else {
				require.NoError(err)
				require.Equal(tt.expected, result)
			}
		})
	}
}
