// Copyright 2020-2021 Dolthub, Inc.
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
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/types"
)

// NOTE: all expected values are pulled from MySQL 8.0

func TestMD5(t *testing.T) {
	tests := []struct {
		val         sql.Expression
		expectedOut string
	}{
		{expression.NewLiteral(int64(1), types.Int64), "c4ca4238a0b923820dcc509a6f75849b"},
		{expression.NewLiteral("1", types.Text), "c4ca4238a0b923820dcc509a6f75849b"},
		{expression.NewLiteral("abcd", types.Text), "e2fc714c4727ee9395f324cd2e7f331f"},
		{expression.NewLiteral(float32(2.5), types.Float32), "8221435bcce913b5c2dc22eaf6cb6590"},
		{expression.NewLiteral("2.5", types.Text), "8221435bcce913b5c2dc22eaf6cb6590"},
		{NewMD5(expression.NewLiteral(int8(10), types.Int8)), "8d8e353b98d5191d5ceea1aa3eb05d43"},
	}

	for _, test := range tests {
		f := NewMD5(test.val)
		t.Run(f.String(), func(t *testing.T) {
			res, err := f.Eval(sql.NewEmptyContext(), nil)
			require.NoError(t, err)
			require.Equal(t, test.expectedOut, res)
		})
	}

	// Test nil
	f := NewMD5(expression.NewLiteral(nil, types.Null))
	t.Run(f.String(), func(t *testing.T) {
		res, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(t, err)
		require.Equal(t, nil, res)
	})
}

func TestSHA1(t *testing.T) {
	tests := []struct {
		val         sql.Expression
		expectedOut string
	}{
		{expression.NewLiteral(int64(1), types.Int64), "356a192b7913b04c54574d18c28d46e6395428ab"},
		{expression.NewLiteral("1", types.Text), "356a192b7913b04c54574d18c28d46e6395428ab"},
		{expression.NewLiteral("abcd", types.Text), "81fe8bfe87576c3ecb22426f8e57847382917acf"},
		{expression.NewLiteral(float32(2.5), types.Float32), "555a5c5c92b230dccab828d90e89ec66847ab9ce"},
		{expression.NewLiteral("2.5", types.Text), "555a5c5c92b230dccab828d90e89ec66847ab9ce"},
		{NewSHA1(expression.NewLiteral(int8(10), types.Int8)), "f270819294d6d015758421bdcb1202fd353c6f06"},
	}

	for _, test := range tests {
		f := NewSHA1(test.val)
		t.Run(f.String(), func(t *testing.T) {
			res, err := f.Eval(sql.NewEmptyContext(), nil)
			require.NoError(t, err)
			require.Equal(t, test.expectedOut, res)
		})
	}

	// Test nil
	f := NewSHA1(expression.NewLiteral(nil, types.Null))
	t.Run(f.String(), func(t *testing.T) {
		res, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(t, err)
		require.Equal(t, nil, res)
	})
}

func TestSHA2(t *testing.T) {
	tests := []struct {
		arg         sql.Expression
		count       sql.Expression
		expectedOut string
	}{
		{
			expression.NewLiteral(int64(1), types.Int64),
			expression.NewLiteral(int64(224), types.Int64),
			"e25388fde8290dc286a6164fa2d97e551b53498dcbf7bc378eb1f178",
		},
		{
			expression.NewLiteral("1", types.Text),
			expression.NewLiteral("224", types.Text),
			"e25388fde8290dc286a6164fa2d97e551b53498dcbf7bc378eb1f178",
		},
		{
			expression.NewLiteral("abcd", types.Text),
			expression.NewPlus(
				expression.NewLiteral(int64(220), types.Int64),
				expression.NewLiteral(int64(4), types.Int64),
			),
			"a76654d8e3550e9a2d67a0eeb6c67b220e5885eddd3fde135806e601",
		},
		{
			expression.NewLiteral(float32(2.5), types.Float32),
			expression.NewLiteral(int64(224), types.Int64),
			"55b3f1e81821cba451fd6c844db119240fd96b2b34dfcca150b8dfd3",
		},
		{
			expression.NewLiteral("2.5", types.Text),
			expression.NewLiteral("224", types.Text),
			"55b3f1e81821cba451fd6c844db119240fd96b2b34dfcca150b8dfd3",
		},
		{
			expression.NewLiteral(int64(1), types.Int64),
			expression.NewLiteral(int64(256), types.Int64),
			"6b86b273ff34fce19d6b804eff5a3f5747ada4eaa22f1d49c01e52ddb7875b4b",
		},
		{
			expression.NewLiteral("1", types.Text),
			expression.NewLiteral("256", types.Text),
			"6b86b273ff34fce19d6b804eff5a3f5747ada4eaa22f1d49c01e52ddb7875b4b",
		},
		{
			expression.NewLiteral("abcd", types.Text),
			expression.NewPlus(
				expression.NewLiteral(int64(250), types.Int64),
				expression.NewLiteral(int64(6), types.Int64),
			),
			"88d4266fd4e6338d13b845fcf289579d209c897823b9217da3e161936f031589",
		},
		{
			expression.NewLiteral(float32(2.5), types.Float32),
			expression.NewLiteral(int64(256), types.Int64),
			"b8736b999909049671d0ea075a42b308a5fbe2df1854899123fe09eb0ee9de61",
		},
		{
			expression.NewLiteral("2.5", types.Text),
			expression.NewLiteral("256", types.Text),
			"b8736b999909049671d0ea075a42b308a5fbe2df1854899123fe09eb0ee9de61",
		},
		{
			expression.NewLiteral(int64(1), types.Int64),
			expression.NewLiteral(int64(0), types.Int64),
			"6b86b273ff34fce19d6b804eff5a3f5747ada4eaa22f1d49c01e52ddb7875b4b",
		},
		{
			expression.NewLiteral("1", types.Text),
			expression.NewLiteral("0", types.Text),
			"6b86b273ff34fce19d6b804eff5a3f5747ada4eaa22f1d49c01e52ddb7875b4b",
		},
		{
			expression.NewLiteral("abcd", types.Text),
			expression.NewLiteral(int64(0), types.Int64),
			"88d4266fd4e6338d13b845fcf289579d209c897823b9217da3e161936f031589",
		},
		{
			expression.NewLiteral(float32(2.5), types.Float32),
			expression.NewLiteral(int64(0), types.Int64),
			"b8736b999909049671d0ea075a42b308a5fbe2df1854899123fe09eb0ee9de61",
		},
		{
			expression.NewLiteral("2.5", types.Text),
			expression.NewLiteral("0", types.Text),
			"b8736b999909049671d0ea075a42b308a5fbe2df1854899123fe09eb0ee9de61",
		},
		{
			expression.NewLiteral(int64(1), types.Int64),
			expression.NewLiteral(int64(384), types.Int64),
			"47f05d367b0c32e438fb63e6cf4a5f35c2aa2f90dc7543f8a41a0f95ce8a40a313ab5cf36134a2068c4c969cb50db776",
		},
		{
			expression.NewLiteral("1", types.Text),
			expression.NewLiteral("384", types.Text),
			"47f05d367b0c32e438fb63e6cf4a5f35c2aa2f90dc7543f8a41a0f95ce8a40a313ab5cf36134a2068c4c969cb50db776",
		},
		{
			expression.NewLiteral("abcd", types.Text),
			expression.NewPlus(
				expression.NewLiteral(int64(380), types.Int64),
				expression.NewLiteral(int64(4), types.Int64),
			),
			"1165b3406ff0b52a3d24721f785462ca2276c9f454a116c2b2ba20171a7905ea5a026682eb659c4d5f115c363aa3c79b",
		},
		{
			expression.NewLiteral(float32(2.5), types.Float32),
			expression.NewLiteral(int64(384), types.Int64),
			"36b9f321bf02e6f84ee38bf6189496a9ee02d081d7197289a2b73cd39e8d8dbcd466599fd6af13f0d79e9d1051f061bc",
		},
		{
			expression.NewLiteral("2.5", types.Text),
			expression.NewLiteral("384", types.Text),
			"36b9f321bf02e6f84ee38bf6189496a9ee02d081d7197289a2b73cd39e8d8dbcd466599fd6af13f0d79e9d1051f061bc",
		},
		{
			expression.NewLiteral(int64(1), types.Int64),
			expression.NewLiteral(int64(512), types.Int64),
			"4dff4ea340f0a823f15d3f4f01ab62eae0e5da579ccb851f8db9dfe84c58b2b37b89903a740e1ee172da793a6e79d560e5f7f9bd058a12a280433ed6fa46510a",
		},
		{
			expression.NewLiteral("1", types.Text),
			expression.NewLiteral("512", types.Text),
			"4dff4ea340f0a823f15d3f4f01ab62eae0e5da579ccb851f8db9dfe84c58b2b37b89903a740e1ee172da793a6e79d560e5f7f9bd058a12a280433ed6fa46510a",
		},
		{
			expression.NewLiteral("abcd", types.Text),
			expression.NewPlus(
				expression.NewLiteral(int64(510), types.Int64),
				expression.NewLiteral(int64(2), types.Int64),
			),
			"d8022f2060ad6efd297ab73dcc5355c9b214054b0d1776a136a669d26a7d3b14f73aa0d0ebff19ee333368f0164b6419a96da49e3e481753e7e96b716bdccb6f",
		},
		{
			expression.NewLiteral(float32(2.5), types.Float32),
			expression.NewLiteral(int64(512), types.Int64),
			"a4281cc49c2503bd0a0876db08ac6280583ebfcee6186c054792d877e7febe63251bfb82616504ed8ee36b146a7d5c6bfcdfcf9c27969a3874bab4544efed501",
		},
		{
			expression.NewLiteral("2.5", types.Text),
			expression.NewLiteral("512", types.Text),
			"a4281cc49c2503bd0a0876db08ac6280583ebfcee6186c054792d877e7febe63251bfb82616504ed8ee36b146a7d5c6bfcdfcf9c27969a3874bab4544efed501",
		},
	}

	for _, test := range tests {
		f := NewSHA2(test.arg, test.count)
		t.Run(f.String(), func(t *testing.T) {
			res, err := f.Eval(sql.NewEmptyContext(), nil)
			require.NoError(t, err)
			require.Equal(t, test.expectedOut, res)
		})
	}
}

func TestSHA2Null(t *testing.T) {
	tests := []struct {
		arg   sql.Expression
		count sql.Expression
	}{
		{
			expression.NewLiteral(nil, types.Null),
			expression.NewLiteral(int64(224), types.Int64),
		},
		{
			expression.NewLiteral("1", types.Text),
			expression.NewLiteral(nil, types.Null),
		},
		{
			expression.NewLiteral(float32(2.5), types.Float32),
			expression.NewLiteral(int64(7), types.Int64),
		},
		{
			expression.NewLiteral(float32(2.5), types.Float32),
			expression.NewLiteral("255", types.Text),
		},
	}

	for _, test := range tests {
		f := NewSHA2(test.arg, test.count)
		t.Run(f.String(), func(t *testing.T) {
			res, err := f.Eval(sql.NewEmptyContext(), nil)
			require.NoError(t, err)
			require.Equal(t, nil, res)
		})
	}
}

func TestCompress(t *testing.T) {
	tests := []struct {
		val sql.Expression
		exp interface{}
	}{
		{
			val: expression.NewLiteral(nil, types.Null),
			exp: nil,
		},
		{
			val: expression.NewLiteral(int64(1), types.Int64),
			exp: "0100000078DA3204040000FFFF00320032",
		},
		{
			val: expression.NewLiteral("1", types.Text),
			exp: "0100000078DA3204040000FFFF00320032",
		},
		{
			val: expression.NewLiteral("", types.Text),
			exp: "",
		},
		{
			val: expression.NewLiteral("abc", types.Text),
			exp: "0300000078DA4A4C4A06040000FFFF024D0127",
		},
		{
			val: expression.NewLiteral("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA", types.Text),
			exp: "2A00000078DA72241A00020000FFFFE5710AAB",
		},
	}

	for _, test := range tests {
		f := NewCompress(test.val)
		t.Run(f.String(), func(t *testing.T) {
			res, err := f.Eval(sql.NewEmptyContext(), nil)
			require.NoError(t, err)
			if test.exp == nil {
				require.Nil(t, res)
				return
			}
			resStr := fmt.Sprintf("%X", res)
			require.Equal(t, test.exp, resStr)
		})
	}
}

func TestUncompress(t *testing.T) {
	tests := []struct {
		val sql.Expression
		exp interface{}
	}{
		{
			val: expression.NewLiteral(nil, types.Null),
			exp: nil,
		},
		{
			val: expression.NewLiteral(int64(1), types.Int64),
			exp: nil,
		},
		{
			val: expression.NewLiteral("1", types.Text),
			exp: nil,
		},
		{
			val: expression.NewLiteral("", types.Text),
			exp: "",
		},
		{
			val: expression.NewLiteral([]byte{0x03, 0x00, 0x00, 0x00, 0x78, 0xDA, 0x4A, 0x4C, 0x4A, 0x06, 0x04, 0x00, 0x00, 0xFF, 0xFF, 0x02, 0x4D, 0x01, 0x27}, types.Blob),
			exp: "616263", // abc
		},
		{
			val: expression.NewLiteral([]byte{0x2A, 0x00, 0x00, 0x00, 0x78, 0xDA, 0x72, 0x24, 0x1A, 0x00, 0x02, 0x00, 0x00, 0xFF, 0xFF, 0xE5, 0x71, 0x0A, 0xAB}, types.Blob),
			exp: "414141414141414141414141414141414141414141414141414141414141414141414141414141414141", //AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA
		},
		{
			val: expression.NewLiteral([]byte{0x0F, 0x00, 0x00, 0x00, 0x78, 0x9C, 0xCB, 0xAD, 0x2C, 0x2E, 0xCC, 0x49, 0xCE, 0xCF, 0x2D, 0x28, 0x4A, 0x2D, 0x2E, 0x4E, 0x4D, 0x01, 0x00, 0x34, 0x0C, 0x06, 0x6C}, types.Blob),
			exp: "6D7973716C636F6D70726573736564", // mysqlcompressed
		},
		{
			// header is too large
			val: expression.NewLiteral([]byte{0xFF, 0x00, 0x00, 0x00, 0x78, 0xDA, 0x4A, 0x4C, 0x4A, 0x06, 0x04, 0x00, 0x00, 0xFF, 0xFF, 0x02, 0x4D, 0x01, 0x27}, types.Blob),
			exp: "616263", // abc
		},
		{
			// header is too small
			val: expression.NewLiteral([]byte{0x02, 0x00, 0x00, 0x00, 0x78, 0xDA, 0x4A, 0x4C, 0x4A, 0x06, 0x04, 0x00, 0x00, 0xFF, 0xFF, 0x02, 0x4D, 0x01, 0x27}, types.Blob),
			exp: nil,
		},
	}

	for _, test := range tests {
		f := NewUncompress(test.val)
		t.Run(f.String(), func(t *testing.T) {
			res, err := f.Eval(sql.NewEmptyContext(), nil)
			require.NoError(t, err)
			if test.exp == nil {
				require.Nil(t, res)
				return
			}
			resStr := fmt.Sprintf("%X", res)
			require.Equal(t, test.exp, resStr)
		})
	}
}

func TestUncompressedLength(t *testing.T) {
	tests := []struct {
		val sql.Expression
		exp interface{}
	}{
		{
			val: expression.NewLiteral(nil, types.Null),
			exp: nil,
		},
		{
			val: expression.NewLiteral(int64(1), types.Int64),
			exp: nil,
		},
		{
			val: expression.NewLiteral("1", types.Text),
			exp: nil,
		},
		{
			val: expression.NewLiteral("", types.Text),
			exp: uint32(0),
		},
		{
			val: expression.NewLiteral([]byte{0x03, 0x00, 0x00, 0x00, 0x78, 0xDA, 0x4A, 0x4C, 0x4A, 0x06, 0x04, 0x00, 0x00, 0xFF, 0xFF, 0x02, 0x4D, 0x01, 0x27}, types.Blob),
			exp: uint32(3),
		},
		{
			val: expression.NewLiteral([]byte{0x2A, 0x00, 0x00, 0x00, 0x78, 0xDA, 0x72, 0x24, 0x1A, 0x00, 0x02, 0x00, 0x00, 0xFF, 0xFF, 0xE5, 0x71, 0x0A, 0xAB}, types.Blob),
			exp: uint32(42),
		},
		{
			val: expression.NewLiteral([]byte{0x0F, 0x00, 0x00, 0x00, 0x78, 0x9C, 0xCB, 0xAD, 0x2C, 0x2E, 0xCC, 0x49, 0xCE, 0xCF, 0x2D, 0x28, 0x4A, 0x2D, 0x2E, 0x4E, 0x4D, 0x01, 0x00, 0x34, 0x0C, 0x06, 0x6C}, types.Blob),
			exp: uint32(15),
		},
		{
			// header is too large
			val: expression.NewLiteral([]byte{0xFF, 0x00, 0x00, 0x00, 0x78, 0xDA, 0x4A, 0x4C, 0x4A, 0x06, 0x04, 0x00, 0x00, 0xFF, 0xFF, 0x02, 0x4D, 0x01, 0x27}, types.Blob),
			exp: uint32(255),
		},
		{
			// header is too small
			val: expression.NewLiteral([]byte{0x02, 0x00, 0x00, 0x00, 0x78, 0xDA, 0x4A, 0x4C, 0x4A, 0x06, 0x04, 0x00, 0x00, 0xFF, 0xFF, 0x02, 0x4D, 0x01, 0x27}, types.Blob),
			exp: uint32(2),
		},
	}

	for _, test := range tests {
		f := NewUncompressedLength(test.val)
		t.Run(f.String(), func(t *testing.T) {
			res, err := f.Eval(sql.NewEmptyContext(), nil)
			require.NoError(t, err)
			require.Equal(t, test.exp, res)
		})
	}
}

func TestValidatePasswordStrength(t *testing.T) {
	tests := []struct {
		val sql.Expression
		exp interface{}
	}{
		{
			val: expression.NewLiteral(nil, types.Null),
			exp: nil,
		},
		{
			val: expression.NewLiteral(int64(1), types.Int64),
			exp: 0,
		},
		{
			val: expression.NewLiteral("1", types.Text),
			exp: 0,
		},
		{
			val: expression.NewLiteral("", types.Text),
			exp: 0,
		},
		{
			val: expression.NewLiteral("weak", types.Text),
			exp: 25,
		},
		{
			val: expression.NewLiteral("lessweak$_@123", types.Text),
			exp: 50,
		},
		{
			val: expression.NewLiteral("N0Tweak$_@123!", types.Text),
			exp: 100,
		},
	}

	for _, test := range tests {
		f := NewValidatePasswordStrength(test.val)
		t.Run(f.String(), func(t *testing.T) {
			res, err := f.Eval(sql.NewEmptyContext(), nil)
			require.NoError(t, err)
			require.Equal(t, test.exp, res)
		})
	}
}
