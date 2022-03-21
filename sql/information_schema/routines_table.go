package information_schema

import (
	"bytes"
	"fmt"
	. "github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
)

type routineTable struct {
	name       string
	schema     Schema
	catalog    Catalog
	procedures []*plan.Procedure
	// functions
	rowIter func(*Context, Catalog, []*plan.Procedure) (RowIter, error)
}

var (
	_ Table = (*routineTable)(nil)
)

func (t *routineTable) AssignCatalog(cat Catalog) Table {
	t.catalog = cat
	return t
}

func (r *routineTable) AssignProcedures(p []*plan.Procedure) Table {
	// TODO: should also assign functions
	r.procedures = p
	return r
}

// Name implements the sql.Table interface.
func (r *routineTable) Name() string {
	return r.name
}

// Schema implements the sql.Table interface.
func (r *routineTable) Schema() Schema {
	return r.schema
}

func (r *routineTable) String() string {
	return printTable(r.Name(), r.Schema())
}

func (r *routineTable) Partitions(context *Context) (PartitionIter, error) {
	return &informationSchemaPartitionIter{informationSchemaPartition: informationSchemaPartition{partitionKey(r.Name())}}, nil
}

func (r *routineTable) PartitionRows(context *Context, partition Partition) (RowIter, error) {
	if !bytes.Equal(partition.Key(), partitionKey(r.Name())) {
		return nil, ErrPartitionNotFound.New(partition.Key())
	}
	if r.rowIter == nil {
		return RowsToRowIter(), nil
	}
	if r.catalog == nil {
		return nil, fmt.Errorf("nil catalog for info schema table %s", r.name)
	}

	return r.rowIter(context, r.catalog, r.procedures)
}

func routinesRowIter(ctx *Context, c Catalog, p []*plan.Procedure) (RowIter, error) {
	var rows []Row
	var (
		securityType    = "DEFINER"
		isDeterministic = ""    // YES or NO
		sqlMode         = "SQL" // SQL, NO SQL, READS SQL DATA, or MODIFIES SQL DATA.
	)

	characterSetClient, err := ctx.GetSessionVariable(ctx, "character_set_client")
	if err != nil {
		return nil, err
	}
	collationConnection, err := ctx.GetSessionVariable(ctx, "collation_connection")
	if err != nil {
		return nil, err
	}
	collationServer, err := ctx.GetSessionVariable(ctx, "collation_server")
	if err != nil {
		return nil, err
	}

	for _, procedure := range p {
		if procedure.SecurityContext == plan.ProcedureSecurityContext_Invoker {
			securityType = "INVOKER"
		}
		rows = append(rows, Row{
			procedure.Name,             // specific_name NOT NULL
			"def",                      // routine_catalog
			"sys",                      // routine_schema
			procedure.Name,             // routine_name NOT NULL
			"PROCEDURE",                // routine_type NOT NULL
			"",                         // data_type
			nil,                        // character_maximum_length
			nil,                        // character_octet_length
			nil,                        // numeric_precision
			nil,                        // numeric_scale
			nil,                        // datetime_precision
			nil,                        // character_set_name
			nil,                        // collation_name
			"",                         // dtd_identifier
			"SQL",                      // routine_body NOT NULL
			procedure.Body.String(),    // routine_definition
			nil,                        // external_name
			"SQL",                      // external_language NOT NULL
			"SQL",                      // parameter_style NOT NULL
			isDeterministic,            // is_deterministic NOT NULL
			"",                         // sql_data_access NOT NULL
			nil,                        // sql_path
			securityType,               // security_type NOT NULL 22
			procedure.CreatedAt.UTC(),  // created NOT NULL
			procedure.ModifiedAt.UTC(), // last_altered NOT NULL
			sqlMode,                    // sql_mode NOT NULL
			procedure.Comment,          // routine_comment NOT NULL
			procedure.Definer,          // definer NOT NULL
			characterSetClient,         // character_set_client NOT NULL
			collationConnection,        // collation_connection NOT NULL
			collationServer,            // database_collation NOT NULL
		})
	}

	// TODO: need to add FUNCTIONS routine_type

	return RowsToRowIter(rows...), nil
}
