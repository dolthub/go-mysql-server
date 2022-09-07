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

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
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
		{"float64 is nil", sql.Float64, sql.Int32, sql.NewRow(nil, nil, nil), nil, nil},
		{"float64 without d", sql.Float64, sql.Int32, sql.NewRow(5555.8, nil, nil), nil, nil},
		{"float64 with d", sql.Float64, sql.Int32, sql.NewRow(5555.855, 4, nil), "5,555.8550", nil},
		{"float64 with super big decimal place", sql.Float64, sql.Int32, sql.NewRow(5555.855, 15, nil), "5,555.855000000000000", nil},
		{"float64 with negative d", sql.Float64, sql.Int32, sql.NewRow(5552.855, -1, nil), "5,553", nil},
		{"float64 with float d", sql.Float64, sql.Float64, sql.NewRow(5555.855, float64(2.123), nil), "5,555.86", nil},
		{"float64 with float negative d", sql.Float64, sql.Float64, sql.NewRow(5552.855, float64(-1), nil), "5,553", nil},
		{"float64 with blob d", sql.Float64, sql.Blob, sql.NewRow(5555.855, []byte{1, 2, 3}, nil), "5,555.855000000000500000000000000000", nil},
		{"float64 with text d", sql.Float64, sql.Text, sql.NewRow(5555.855, "2", nil), "5,555.86", nil},
		{"negative float64 with d", sql.Float64, sql.Int32, sql.NewRow(-5555.855, 2, nil), "-5,555.86", nil},
		{"blob is nil", sql.Blob, sql.Int32, sql.NewRow(nil, nil, nil), nil, nil},
		{"blob is ok", sql.Blob, sql.Int32, sql.NewRow([]byte{1, 2, 3}, nil, nil), nil, nil},
		{"text int without d", sql.Text, sql.Int32, sql.NewRow("98765432", nil, nil), nil, nil},
		{"text int with d", sql.Text, sql.Int32, sql.NewRow("98765432", 2, nil), "98,765,432.00", nil},
		{"text int with negative d", sql.Text, sql.Int32, sql.NewRow("98765432", -1, nil), "98,765,432", nil},
		{"text int with float d", sql.Text, sql.Float64, sql.NewRow("98765432", 2.123, nil), "98,765,432.00", nil},
		{"text int with float negative d", sql.Text, sql.Float64, sql.NewRow("98765432", float32(-1), nil), "98,765,432", nil},
		{"text float without d", sql.Text, sql.Int32, sql.NewRow("98765432.1234", nil, nil), nil, nil},
		{"text float with d", sql.Text, sql.Int32, sql.NewRow("98765432.1234", 2, nil), "98,765,432.12", nil},
		{"text float with negative d", sql.Text, sql.Int32, sql.NewRow("98765432.8234", -1, nil), "98,765,433", nil},
		{"text float with float d", sql.Text, sql.Float64, sql.NewRow("98765432.1234", float64(2.823), nil), "98,765,432.123", nil},
		{"text float with float negative d", sql.Text, sql.Float64, sql.NewRow("98765432.1234", float64(-1), nil), "98,765,432", nil},
		{"text float with blob d", sql.Text, sql.Blob, sql.NewRow("98765432.1234", []byte{1, 2, 3}, nil), "98,765,432.123400020000000000000000000000", nil},
		{"negative num text int with d", sql.Text, sql.Int32, sql.NewRow("-98765432", 2, nil), "-98,765,432.00", nil},
		{"sci-notn big num", sql.Float64, sql.Int32, sql.NewRow(5932886+.000000000001, 1, nil), "5,932,886.0", nil},
		{"sci-notn big num with big dp", sql.Float64, sql.Int32, sql.NewRow(5932886+.000000000001, 8, nil), "5,932,886.00000000", nil},
		{"sci-notn big exp num", sql.Float64, sql.Int32, sql.NewRow(5.932887e+08, 2, nil), "593,288,700.00", nil},
		{"sci-notn neg big exp num", sql.Float64, sql.Int32, sql.NewRow(-5.932887e+08, 2, nil), "-593,288,700.00", nil},
		{"sci-notn text big exp num", sql.Text, sql.Int32, sql.NewRow("5.932887e+07", 3, nil), "59,328,870.000", nil},
		{"sci-notn text neg big exp num", sql.Text, sql.Int32, sql.NewRow("-5.932887e+08", 2, nil), "-593,288,700.00", nil},
		{"sci-notn exp small num", sql.Float64, sql.Int32, sql.NewRow(5.932887e-08, 2, nil), "0.00", nil},
		{"sci-notn exp small num with big dp", sql.Float64, sql.Int32, sql.NewRow(5.932887e-08, 9, nil), "0.000000059", nil},
		{"sci-notn neg exp small num", sql.Float64, sql.Int32, sql.NewRow(-5.932887e-08, 2, nil), "0.00", nil},
		{"sci-notn neg exp small num with big dp", sql.Float64, sql.Int32, sql.NewRow(-5.932887e-08, 8, nil), "-0.00000006", nil},
		{"sci-notn text neg exp small num", sql.Float64, sql.Int32, sql.NewRow("-5.932887e-08", 2, nil), "0.00", nil},
		{"sci-notn text neg exp small num with big dp", sql.Float64, sql.Int32, sql.NewRow("-5.932887e-08", 8, nil), "-0.00000006", nil},
		{"float64 with loc=ar_AE", sql.Float64, sql.Int32, sql.NewRow(2409384.855, 4, "ar_AE"), "2,409,384.8550", nil},
		{"float64 with loc=ar_BH", sql.Float64, sql.Int32, sql.NewRow(2409384.855, 4, "ar_BH"), "2,409,384.8550", nil},
		{"float64 with loc=ar_EG", sql.Float64, sql.Int32, sql.NewRow(2409384.855, 4, "ar_EG"), "2,409,384.8550", nil},
		{"float64 with loc=ar_IQ", sql.Float64, sql.Int32, sql.NewRow(2409384.855, 4, "ar_IQ"), "2,409,384.8550", nil},
		{"float64 with loc=ar_JO", sql.Float64, sql.Int32, sql.NewRow(2409384.855, 4, "ar_JO"), "2,409,384.8550", nil},
		{"float64 with loc=ar_KW", sql.Float64, sql.Int32, sql.NewRow(2409384.855, 4, "ar_KW"), "2,409,384.8550", nil},
		{"float64 with loc=ar_OM", sql.Float64, sql.Int32, sql.NewRow(2409384.855, 4, "ar_OM"), "2,409,384.8550", nil},
		{"float64 with loc=ar_QA", sql.Float64, sql.Int32, sql.NewRow(2409384.855, 4, "ar_QA"), "2,409,384.8550", nil},
		{"float64 with loc=ar_SD", sql.Float64, sql.Int32, sql.NewRow(2409384.855, 4, "ar_SD"), "2,409,384.8550", nil},
		{"float64 with loc=ar_SY", sql.Float64, sql.Int32, sql.NewRow(2409384.855, 4, "ar_SY"), "2,409,384.8550", nil},
		{"float64 with loc=ar_YE", sql.Float64, sql.Int32, sql.NewRow(2409384.855, 4, "ar_YE"), "2,409,384.8550", nil},
		{"float64 with loc=da_DK", sql.Float64, sql.Int32, sql.NewRow(2409384.855, 4, "da_DK"), "2.409.384,8550", nil},
		{"float64 with loc=de_BE", sql.Float64, sql.Int32, sql.NewRow(2409384.855, 4, "de_BE"), "2.409.384,8550", nil},
		{"float64 with loc=de_DE", sql.Float64, sql.Int32, sql.NewRow(2409384.855, 4, "de_DE"), "2.409.384,8550", nil},
		{"float64 with loc=de_LU", sql.Float64, sql.Int32, sql.NewRow(2409384.855, 4, "de_LU"), "2.409.384,8550", nil},
		{"float64 with loc=en_AU", sql.Float64, sql.Int32, sql.NewRow(2409384.855, 4, "en_AU"), "2,409,384.8550", nil},
		{"float64 with loc=en_CA", sql.Float64, sql.Int32, sql.NewRow(2409384.855, 4, "en_CA"), "2,409,384.8550", nil},
		{"float64 with loc=en_GB", sql.Float64, sql.Int32, sql.NewRow(2409384.855, 4, "en_GB"), "2,409,384.8550", nil},
		{"float64 with loc=en_IN", sql.Float64, sql.Int32, sql.NewRow(2409384.855, 4, "en_IN"), "24,09,384.8550", nil},
		{"float64 with loc=en_NZ", sql.Float64, sql.Int32, sql.NewRow(2409384.855, 4, "en_NZ"), "2,409,384.8550", nil},
		{"float64 with loc=en_PH", sql.Float64, sql.Int32, sql.NewRow(2409384.855, 4, "en_PH"), "2,409,384.8550", nil},
		{"float64 with loc=en_US", sql.Float64, sql.Int32, sql.NewRow(2409384.855, 4, "en_US"), "2,409,384.8550", nil},
		{"float64 with loc=en_ZW", sql.Float64, sql.Int32, sql.NewRow(2409384.855, 4, "en_ZW"), "2,409,384.8550", nil},
		{"float64 with loc=es_AR", sql.Float64, sql.Int32, sql.NewRow(2409384.855, 4, "es_AR"), "2.409.384,8550", nil},
		{"float64 with loc=es_US", sql.Float64, sql.Int32, sql.NewRow(2409384.855, 4, "es_US"), "2,409,384.8550", nil},
		{"float64 with loc=fo_FO", sql.Float64, sql.Int32, sql.NewRow(2409384.855, 4, "fo_FO"), "2.409.384,8550", nil},
		{"float64 with loc=he_IL", sql.Float64, sql.Int32, sql.NewRow(2409384.855, 4, "he_IL"), "2,409,384.8550", nil},
		{"float64 with loc=id_ID", sql.Float64, sql.Int32, sql.NewRow(2409384.855, 4, "id_ID"), "2.409.384,8550", nil},
		{"float64 with loc=is_IS", sql.Float64, sql.Int32, sql.NewRow(2409384.855, 4, "is_IS"), "2.409.384,8550", nil},
		{"float64 with loc=ja_JP", sql.Float64, sql.Int32, sql.NewRow(2409384.855, 4, "ja_JP"), "2,409,384.8550", nil},
		{"float64 with loc=ko_KR", sql.Float64, sql.Int32, sql.NewRow(2409384.855, 4, "ko_KR"), "2,409,384.8550", nil},
		{"float64 with loc=ms_MY", sql.Float64, sql.Int32, sql.NewRow(2409384.855, 4, "ms_MY"), "2,409,384.8550", nil},
		{"float64 with loc=ro_RO", sql.Float64, sql.Int32, sql.NewRow(2409384.855, 4, "ro_RO"), "2.409.384,8550", nil},
		{"float64 with loc=ta_IN", sql.Float64, sql.Int32, sql.NewRow(2409384.855, 4, "ta_IN"), "24,09,384.8550", nil},
		{"float64 with loc=th_TH", sql.Float64, sql.Int32, sql.NewRow(2409384.855, 4, "th_TH"), "2,409,384.8550", nil},
		{"float64 with loc=tr_TR", sql.Float64, sql.Int32, sql.NewRow(2409384.855, 4, "tr_TR"), "2.409.384,8550", nil},
		{"float64 with loc=ur_PK", sql.Float64, sql.Int32, sql.NewRow(2409384.855, 4, "ur_PK"), "2,409,384.8550", nil},
		{"float64 with loc=vi_VN", sql.Float64, sql.Int32, sql.NewRow(2409384.855, 4, "vi_VN"), "2.409.384,8550", nil},
		{"float64 with loc=zh_CN", sql.Float64, sql.Int32, sql.NewRow(2409384.855, 4, "zh_CN"), "2,409,384.8550", nil},
		{"float64 with loc=zh_HK", sql.Float64, sql.Int32, sql.NewRow(2409384.855, 4, "zh_HK"), "2,409,384.8550", nil},
		{"float64 with loc=zh_TW", sql.Float64, sql.Int32, sql.NewRow(2409384.855, 4, "zh_TW"), "2,409,384.8550", nil},
	}

	for _, tt := range testCases {
		var args = make([]sql.Expression, 3)
		args[0] = expression.NewGetField(0, tt.xType, "Val", false)
		args[1] = expression.NewGetField(1, tt.dType, "Df", false)
		args[2] = expression.NewGetField(2, sql.LongText, "Locale", true)
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
		{"sci-notn big num with big dp", sql.Float64, sql.Int32, sql.NewRow(5932886+.000000000001, 15, nil), "5,932,886.000000000001000", nil},
		{"sci-notn text big num", sql.Text, sql.Int32, sql.NewRow("5932886+.000000000001", 1, nil), "5,932,886.0", nil},
		{"float64 with loc=ar_DZ", sql.Float64, sql.Int32, sql.NewRow(2409384.855, 4, "ar_DZ"), "2,409,384.8550", nil},
		{"float64 with loc=ar_IN", sql.Float64, sql.Int32, sql.NewRow(2409384.855, 4, "ar_IN"), "2,409,384.8550", nil},
		{"float64 with loc=ar_LB", sql.Float64, sql.Int32, sql.NewRow(2409384.855, 4, "ar_LB"), "2,409,384.8550", nil},
		{"float64 with loc=ar_LY", sql.Float64, sql.Int32, sql.NewRow(2409384.855, 4, "ar_LY"), "2,409,384.8550", nil},
		{"float64 with loc=ar_MA", sql.Float64, sql.Int32, sql.NewRow(2409384.855, 4, "ar_MA"), "2,409,384.8550", nil},
		{"float64 with loc=ar_SA", sql.Float64, sql.Int32, sql.NewRow(2409384.855, 4, "ar_SA"), "2409384.8550", nil},
		{"float64 with loc=ar_TN", sql.Float64, sql.Int32, sql.NewRow(2409384.855, 4, "ar_TN"), "2,409,384.8550", nil},
		{"float64 with loc=be_BY", sql.Float64, sql.Int32, sql.NewRow(2409384.855, 4, "be_BY"), "2.409.384,8550", nil},
		{"float64 with loc=bg_BG", sql.Float64, sql.Int32, sql.NewRow(2409384.855, 4, "bg_BG"), "2409384,8550", nil},
		{"float64 with loc=ca_ES", sql.Float64, sql.Int32, sql.NewRow(2409384.855, 4, "ca_ES"), "2409384,8550", nil},
		{"float64 with loc=cs_CZ", sql.Float64, sql.Int32, sql.NewRow(2409384.855, 4, "cs_CZ"), "2409384,8550", nil},
		{"float64 with loc=de_AT", sql.Float64, sql.Int32, sql.NewRow(2409384.855, 4, "de_AT"), "2409384,8550", nil},
		{"float64 with loc=de_CH", sql.Float64, sql.Int32, sql.NewRow(2409384.855, 4, "de_CH"), "2'409'384.8550", nil},
		{"float64 with loc=el_GR", sql.Float64, sql.Int32, sql.NewRow(2409384.855, 4, "el_GR"), "2409384,8550", nil},
		{"float64 with loc=en_ZA", sql.Float64, sql.Int32, sql.NewRow(2409384.855, 4, "en_ZA"), "2,409,384.8550", nil},
		{"float64 with loc=es_BO", sql.Float64, sql.Int32, sql.NewRow(2409384.855, 4, "es_BO"), "2409384,8550", nil},
		{"float64 with loc=es_CL", sql.Float64, sql.Int32, sql.NewRow(2409384.855, 4, "es_CL"), "2409384,8550", nil},
		{"float64 with loc=es_CO", sql.Float64, sql.Int32, sql.NewRow(2409384.855, 4, "es_CO"), "2409384,8550", nil},
		{"float64 with loc=es_CR", sql.Float64, sql.Int32, sql.NewRow(2409384.855, 4, "es_CR"), "2409384.8550", nil},
		{"float64 with loc=es_DO", sql.Float64, sql.Int32, sql.NewRow(2409384.855, 4, "es_DO"), "2409384.8550", nil},
		{"float64 with loc=es_EC", sql.Float64, sql.Int32, sql.NewRow(2409384.855, 4, "es_EC"), "2409384,8550", nil},
		{"float64 with loc=es_ES", sql.Float64, sql.Int32, sql.NewRow(2409384.855, 4, "es_ES"), "2409384,8550", nil},
		{"float64 with loc=es_GT", sql.Float64, sql.Int32, sql.NewRow(2409384.855, 4, "es_GT"), "2409384.8550", nil},
		{"float64 with loc=es_HN", sql.Float64, sql.Int32, sql.NewRow(2409384.855, 4, "es_HN"), "2409384.8550", nil},
		{"float64 with loc=es_MX", sql.Float64, sql.Int32, sql.NewRow(2409384.855, 4, "es_MX"), "2409384.8550", nil},
		{"float64 with loc=es_NI", sql.Float64, sql.Int32, sql.NewRow(2409384.855, 4, "es_NI"), "2409384.8550", nil},
		{"float64 with loc=es_PA", sql.Float64, sql.Int32, sql.NewRow(2409384.855, 4, "es_PA"), "2409384.8550", nil},
		{"float64 with loc=es_PE", sql.Float64, sql.Int32, sql.NewRow(2409384.855, 4, "es_PE"), "2409384.8550", nil},
		{"float64 with loc=es_PR", sql.Float64, sql.Int32, sql.NewRow(2409384.855, 4, "es_PR"), "2409384.8550", nil},
		{"float64 with loc=es_PY", sql.Float64, sql.Int32, sql.NewRow(2409384.855, 4, "es_PY"), "2409384,8550", nil},
		{"float64 with loc=es_SV", sql.Float64, sql.Int32, sql.NewRow(2409384.855, 4, "es_SV"), "2409384.8550", nil},
		{"float64 with loc=es_UY", sql.Float64, sql.Int32, sql.NewRow(2409384.855, 4, "es_UY"), "2409384,8550", nil},
		{"float64 with loc=es_VE", sql.Float64, sql.Int32, sql.NewRow(2409384.855, 4, "es_VE"), "2409384,8550", nil},
		{"float64 with loc=et_EE", sql.Float64, sql.Int32, sql.NewRow(2409384.855, 4, "et_EE"), "2 409 384,8550", nil},
		{"float64 with loc=eu_ES", sql.Float64, sql.Int32, sql.NewRow(2409384.855, 4, "eu_ES"), "2409384,8550", nil},
		{"float64 with loc=fi_FI", sql.Float64, sql.Int32, sql.NewRow(2409384.855, 4, "fi_FI"), "2 409 384,8550", nil},
		{"float64 with loc=fr_BE", sql.Float64, sql.Int32, sql.NewRow(2409384.855, 4, "fr_BE"), "2409384,8550", nil},
		{"float64 with loc=fr_CA", sql.Float64, sql.Int32, sql.NewRow(2409384.855, 4, "fr_CA"), "2409384,8550", nil},
		{"float64 with loc=fr_CH", sql.Float64, sql.Int32, sql.NewRow(2409384.855, 4, "fr_CH"), "2409384,8550", nil},
		{"float64 with loc=fr_FR", sql.Float64, sql.Int32, sql.NewRow(2409384.855, 4, "fr_FR"), "2409384,8550", nil},
		{"float64 with loc=fr_LU", sql.Float64, sql.Int32, sql.NewRow(2409384.855, 4, "fr_LU"), "2409384,8550", nil},
		{"float64 with loc=gl_ES", sql.Float64, sql.Int32, sql.NewRow(2409384.855, 4, "gl_ES"), "2409384,8550", nil},
		{"float64 with loc=gu_IN", sql.Float64, sql.Int32, sql.NewRow(2409384.855, 4, "gu_IN"), "2,409,384.8550", nil},
		{"float64 with loc=hi_IN", sql.Float64, sql.Int32, sql.NewRow(2409384.855, 4, "hi_IN"), "2,409,384.8550", nil},
		{"float64 with loc=hr_HR", sql.Float64, sql.Int32, sql.NewRow(2409384.855, 4, "hr_HR"), "2409384,8550", nil},
		{"float64 with loc=hu_HU", sql.Float64, sql.Int32, sql.NewRow(2409384.855, 4, "hu_HU"), "2.409.384,8550", nil},
		{"float64 with loc=it_CH", sql.Float64, sql.Int32, sql.NewRow(2409384.855, 4, "it_CH"), "2'409'384,8550", nil},
		{"float64 with loc=it_IT", sql.Float64, sql.Int32, sql.NewRow(2409384.855, 4, "it_IT"), "2409384,8550", nil},
		{"float64 with loc=lt_LT", sql.Float64, sql.Int32, sql.NewRow(2409384.855, 4, "lt_LT"), "2.409.384,8550", nil},
		{"float64 with loc=lv_LV", sql.Float64, sql.Int32, sql.NewRow(2409384.855, 4, "lv_LV"), "2 409 384,8550", nil},
		{"float64 with loc=mk_MK", sql.Float64, sql.Int32, sql.NewRow(2409384.855, 4, "mk_MK"), "2 409 384,8550", nil},
		{"float64 with loc=mn_MN", sql.Float64, sql.Int32, sql.NewRow(2409384.855, 4, "mn_MN"), "2.409.384,8550", nil},
		{"float64 with loc=nb_NO", sql.Float64, sql.Int32, sql.NewRow(2409384.855, 4, "nb_NO"), "2.409.384,8550", nil},
		{"float64 with loc=nl_BE", sql.Float64, sql.Int32, sql.NewRow(2409384.855, 4, "nl_BE"), "2409384,8550", nil},
		{"float64 with loc=nl_NL", sql.Float64, sql.Int32, sql.NewRow(2409384.855, 4, "nl_NL"), "2409384,8550", nil},
		{"float64 with loc=no_NO", sql.Float64, sql.Int32, sql.NewRow(2409384.855, 4, "no_NO"), "2.409.384,8550", nil},
		{"float64 with loc=pl_PL", sql.Float64, sql.Int32, sql.NewRow(2409384.855, 4, "pl_PL"), "2409384,8550", nil},
		{"float64 with loc=pt_BR", sql.Float64, sql.Int32, sql.NewRow(2409384.855, 4, "pt_BR"), "2409384,8550", nil},
		{"float64 with loc=pt_PT", sql.Float64, sql.Int32, sql.NewRow(2409384.855, 4, "pt_PT"), "2409384,8550", nil},
		{"float64 with loc=rm_CH", sql.Float64, sql.Int32, sql.NewRow(2409384.855, 4, "rm_CH"), "2'409'384,8550", nil},
		{"float64 with loc=ru_RU", sql.Float64, sql.Int32, sql.NewRow(2409384.855, 4, "ru_RU"), "2 409 384,8550", nil},
		{"float64 with loc=ru_UA", sql.Float64, sql.Int32, sql.NewRow(2409384.855, 4, "ru_UA"), "2.409.384,8550", nil},
		{"float64 with loc=sk_SK", sql.Float64, sql.Int32, sql.NewRow(2409384.855, 4, "sk_SK"), "2 409 384,8550", nil},
		{"float64 with loc=sl_SI", sql.Float64, sql.Int32, sql.NewRow(2409384.855, 4, "sl_SI"), "2409384,8550", nil},
		{"float64 with loc=sq_AL", sql.Float64, sql.Int32, sql.NewRow(2409384.855, 4, "sq_AL"), "2.409.384,8550", nil},
		{"float64 with loc=sr_RS", sql.Float64, sql.Int32, sql.NewRow(2409384.855, 4, "sr_RS"), "2409384.8550", nil},
		{"float64 with loc=sv_FI", sql.Float64, sql.Int32, sql.NewRow(2409384.855, 4, "sv_FI"), "2 409 384,8550", nil},
		{"float64 with loc=sv_SE", sql.Float64, sql.Int32, sql.NewRow(2409384.855, 4, "sv_SE"), "2 409 384,8550", nil},
		{"float64 with loc=te_IN", sql.Float64, sql.Int32, sql.NewRow(2409384.855, 4, "te_IN"), "24,09,384.8550", nil},
		{"float64 with loc=uk_UA", sql.Float64, sql.Int32, sql.NewRow(2409384.855, 4, "uk_UA"), "2.409.384,8550", nil},
	}

	for _, tt := range testCases {
		var args = make([]sql.Expression, 3)
		args[0] = expression.NewGetField(0, tt.xType, "Val", false)
		args[1] = expression.NewGetField(1, tt.dType, "Df", false)
		args[2] = expression.NewGetField(2, sql.LongText, "Locale", true)
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
