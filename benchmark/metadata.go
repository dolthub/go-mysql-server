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

package benchmark

import "github.com/dolthub/go-mysql-server/sql"

type tableMetadata struct {
	schema sql.PrimaryKeySchema
	name   string
}

var tpchTableMetadata = []tableMetadata{{
	name: "part",
	schema: sql.NewPrimaryKeySchema(sql.Schema{
		{
			Name:     "p_partkey",
			Nullable: true,
			Type:     sql.Int64,
		},
		{
			Name:     "p_name",
			Nullable: true,
			Type:     sql.Text,
		},
		{
			Name:     "p_mfgr",
			Nullable: true,
			Type:     sql.Text,
		},
		{
			Name:     "p_brand",
			Nullable: true,
			Type:     sql.Text,
		},
		{
			Name:     "p_type",
			Nullable: true,
			Type:     sql.Text,
		},
		{
			Name:     "p_size",
			Nullable: true,
			Type:     sql.Int32,
		},
		{
			Name:     "p_continer",
			Nullable: true,
			Type:     sql.Text,
		},
		{
			Name:     "p_retailprice",
			Nullable: true,
			Type:     sql.Float64,
		},
		{
			Name:     "p_comment",
			Nullable: true,
			Type:     sql.Text,
		},
	}),
},
	{
		name: "supplier",
		schema: sql.NewPrimaryKeySchema(sql.Schema{
			{
				Name:     "s_supkey",
				Nullable: true,
				Type:     sql.Int64,
			},
			{
				Name:     "s_name",
				Nullable: true,
				Type:     sql.Text,
			},
			{
				Name:     "s_address",
				Nullable: true,
				Type:     sql.Text,
			},
			{
				Name:     "s_nationkey",
				Nullable: true,
				Type:     sql.Int64,
			},
			{
				Name:     "s_phone",
				Nullable: true,
				Type:     sql.Text,
			},
			{
				Name:     "s_actbal",
				Nullable: true,
				Type:     sql.Float64,
			},
			{
				Name:     "s_comment",
				Nullable: true,
				Type:     sql.Text,
			},
		}),
	},
	{
		name: "partsupp",
		schema: sql.NewPrimaryKeySchema(sql.Schema{
			{
				Name:     "ps_partkey",
				Nullable: true,
				Type:     sql.Int64,
			},
			{
				Name:     "ps_suppkey",
				Nullable: true,
				Type:     sql.Int64,
			},
			{
				Name:     "ps_availqty",
				Nullable: true,
				Type:     sql.Int32,
			},
			{
				Name:     "ps_supplycost",
				Nullable: true,
				Type:     sql.Float64,
			},
			{
				Name:     "ps_comment",
				Nullable: true,
				Type:     sql.Text,
			},
		}),
	},
	{
		name: "customer",
		schema: sql.NewPrimaryKeySchema(sql.Schema{
			{
				Name:     "c_custkey",
				Nullable: true,
				Type:     sql.Int64,
			},
			{
				Name:     "c_name",
				Nullable: true,
				Type:     sql.Text,
			},
			{
				Name:     "c_address",
				Nullable: true,
				Type:     sql.Text,
			},
			{
				Name:     "c_nationkey",
				Nullable: true,
				Type:     sql.Int64,
			},
			{
				Name:     "c_phone",
				Nullable: true,
				Type:     sql.Text,
			},
			{
				Name:     "c_acctbal",
				Nullable: true,
				Type:     sql.Float64,
			},
			{
				Name:     "c_mktsegement",
				Nullable: true,
				Type:     sql.Text,
			},
			{
				Name:     "c_comment",
				Nullable: true,
				Type:     sql.Text,
			},
		}),
	},
	{
		name: "orders",
		schema: sql.NewPrimaryKeySchema(sql.Schema{
			{
				Name:     "o_orderkey",
				Nullable: true,
				Type:     sql.Int64,
			},
			{
				Name:     "o_custkey",
				Nullable: true,
				Type:     sql.Int64,
			},
			{
				Name:     "o_orderstatus",
				Nullable: true,
				Type:     sql.Text,
			},
			{
				Name:     "o_totalprice",
				Nullable: true,
				Type:     sql.Float64,
			},
			{
				Name:     "o_orderdate",
				Nullable: true,
				// TODO: value "1996-01-02" can't be converted to time.Time
				// Type:     sql.Timestamp,
				Type: sql.Text,
			},
			{
				Name:     "o_orderpriority",
				Nullable: true,
				Type:     sql.Text,
			},
			{
				Name:     "o_clerk",
				Nullable: true,
				Type:     sql.Text,
			},
			{
				Name:     "o_shippriority",
				Nullable: true,
				Type:     sql.Int32,
			},
			{
				Name:     "o_comment",
				Nullable: true,
				Type:     sql.Text,
			},
		}),
	},
	{
		name: "lineitem",
		schema: sql.NewPrimaryKeySchema(sql.Schema{
			{
				Name:     "l_orderkey",
				Nullable: true,
				Type:     sql.Int64,
			},
			{
				Name:     "l_partkey",
				Nullable: true,
				Type:     sql.Int64,
			},
			{
				Name:     "l_suppkey",
				Nullable: true,
				Type:     sql.Int64,
			},
			{
				Name:     "l_linenumber",
				Nullable: true,
				Type:     sql.Int32,
			},
			{
				Name:     "l_quantity",
				Nullable: true,
				Type:     sql.Float64,
			},
			{
				Name:     "l_extendedprice",
				Nullable: true,
				Type:     sql.Float64,
			},
			{
				Name:     "l_discount",
				Nullable: true,
				Type:     sql.Float64,
			},
			{
				Name:     "l_tax",
				Nullable: true,
				Type:     sql.Float64,
			},
			{
				Name:     "l_returnflag",
				Nullable: true,
				Type:     sql.Text,
			},
			{
				Name:     "l_linestatus",
				Nullable: true,
				Type:     sql.Text,
			},
			{
				Name:     "l_shipdate",
				Nullable: true,
				// TODO: value "1996-03-13" can't be converted to time.Time
				// Type: sql.Timestamp,
				Type: sql.Text,
			},
			{
				Name:     "l_commitdate",
				Nullable: true,
				// TODO: value "1996-03-13" can't be converted to time.Time
				// Type: sql.Timestamp,
				Type: sql.Text,
			},
			{
				Name:     "l_receiptdate",
				Nullable: true,
				// TODO: value "1996-03-13" can't be converted to time.Time
				// Type: sql.Timestamp,
				Type: sql.Text,
			},
			{
				Name:     "l_shipinstruct",
				Nullable: true,
				Type:     sql.Text,
			},
			{
				Name:     "l_shipmode",
				Nullable: true,
				Type:     sql.Text,
			},
			{
				Name:     "l_comment",
				Nullable: true,
				Type:     sql.Text,
			},
		}),
	},
	{
		name: "nation",
		schema: sql.NewPrimaryKeySchema(sql.Schema{
			{
				Name:     "n_nationkey",
				Nullable: true,
				Type:     sql.Int64,
			},
			{
				Name:     "n_name",
				Nullable: true,
				Type:     sql.Text,
			},
			{
				Name:     "n_regionkey",
				Nullable: true,
				Type:     sql.Int64,
			},
			{
				Name:     "n_comment",
				Nullable: true,
				Type:     sql.Text,
			},
		}),
	},
	{
		name: "region",
		schema: sql.NewPrimaryKeySchema(sql.Schema{
			{
				Name:     "r_regionkey",
				Nullable: true,
				Type:     sql.Int64,
			},
			{
				Name:     "r_name",
				Nullable: true,
				Type:     sql.Text,
			},
			{
				Name:     "r_comment",
				Nullable: true,
				Type:     sql.Text,
			},
		}),
	},
}
