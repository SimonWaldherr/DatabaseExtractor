package main

import (
	"database/sql"
	"fmt"

	_ "github.com/denisenkom/go-mssqldb"
)

// TableInfo represents a table or view
type TableInfo struct {
	Database     string
	Schema       string
	TableName    string
	Definition   string
	Columns      []Column
	Dependencies []Dependency
	Type         string
}

// Dependency represents a dependency of a table or view
type Dependency struct {
	ReferencedDB     string
	ReferencedSchema string
	ReferencedTable  string
}

// Column represents a column in a table or view
type Column struct {
	Name           string
	Type_Name      string
	Max_Length     int
	Precision      int
	Scale          int
	Collation_Name string
	Is_Nullable    bool
	Is_Identity    bool
}

// Database represents a map with the database name as key and a list of tables/views as value
type Database map[string][]TableInfo

// sqlQueries contains the SQL queries used to query the database
var sqlQueries = map[string]string{
	"queryTables":            "SELECT TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE FROM [%s].INFORMATION_SCHEMA.TABLES UNION ALL SELECT ROUTINE_CATALOG, ROUTINE_SCHEMA, ROUTINE_NAME, ROUTINE_TYPE FROM [%s].INFORMATION_SCHEMA.ROUTINES",
	"queryColumns":           "USE %s; SELECT c.Name, [Type_Name] = tp.name, c.Max_Length, c.[Precision], c.Scale, ISNULL(c.Collation_Name, '') as Collation_Name, c.Is_Nullable, c.Is_Identity FROM sys.columns c WITH(NOLOCK) JOIN sys.types tp WITH(NOLOCK) ON c.user_type_id = tp.user_type_id WHERE c.[object_id] = OBJECT_ID(N'[%s].[%s].[%s]')",
	"queryViewDefinition":    "USE %s; SELECT ISNULL(OBJECT_DEFINITION(OBJECT_ID(N'[%s].[%s].[%s]')),'') as [definition]",
	"queryTableDependencies": "SELECT ISNULL(referenced_database_name, '') as referenced_database_name, ISNULL(referenced_schema_name,'') as referenced_schema_name, ISNULL(referenced_entity_name,'') as referenced_entity_name FROM [%s].sys.sql_expression_dependencies WHERE referencing_id = OBJECT_ID(N'[%s].[%s].[%s]')",
}

var mysqlQueries = map[string]string{
	"queryTables":            "SELECT TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE FROM information_schema.TABLES UNION ALL SELECT ROUTINE_CATALOG, ROUTINE_SCHEMA, ROUTINE_NAME, ROUTINE_TYPE FROM information_schema.ROUTINES",
	"queryColumns":           "USE %s; SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION, NUMERIC_SCALE, COLLATION_NAME, IS_NULLABLE, COLUMN_KEY FROM information_schema.columns WHERE TABLE_SCHEMA = '%s' AND TABLE_NAME = '%s'",
	"queryViewDefinition":    "USE %s; SHOW CREATE VIEW `%s`.`%s`",
	"queryTableDependencies": "SELECT REFERENCED_TABLE_SCHEMA, REFERENCED_TABLE_NAME FROM information_schema.KEY_COLUMN_USAGE WHERE TABLE_SCHEMA = '%s' AND TABLE_NAME = '%s' AND REFERENCED_TABLE_NAME IS NOT NULL",
}

// typeMap maps the type names from the database to the type names used in the information file
var typeMap = map[string]string{
	"BASE TABLE": "Table",
	"VIEW":       "View",
	"FUNCTION":   "Function",
	"PROCEDURE":  "Procedure",
}

// queryDatabases queries the given databases and returns a list of TableInfo
func queryDatabases(config Config) ([]TableInfo, error) {
	connString := fmt.Sprintf("server=%s;user id=%s;password=%s;port=1433", config.Server, config.User, config.Password)
	db, err := sql.Open("mssql", connString)
	if err != nil {
		return nil, err
	}
	defer db.Close()

	var results []TableInfo
	for _, database := range config.Databases {
		fmt.Printf("Database %s: \n", database)
		tables, err := queryTables(db, database)
		if err != nil {
			return nil, err
		}
		results = append(results, tables...)
	}

	return results, nil
}

// queryTables queries the tables of the given database and returns a list of TableInfo
func queryTables(db *sql.DB, database string) ([]TableInfo, error) {
	query := fmt.Sprintf(sqlQueries["queryTables"], database, database)
	rows, err := db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tables []TableInfo

	for rows.Next() {
		var dbn, schema, tableName, typen string

		if err := rows.Scan(&dbn, &schema, &tableName, &typen); err != nil {
			return nil, err
		}

		fmt.Printf("Database %s, Schema: %s, %s: %s \n", database, schema, typeMap[typen], tableName)

		definition, err := queryViewDefinition(db, database, schema, tableName)
		if err != nil {
			return nil, err
		}

		tablestruct, err := queryTableDefinition(db, database, schema, tableName)
		if err != nil {
			return nil, err
		}

		dependencies, err := queryTableDependencies(db, database, schema, tableName)
		if err != nil {
			return nil, err
		}

		tables = append(tables, TableInfo{
			Database:     database,
			Schema:       schema,
			TableName:    tableName,
			Definition:   definition,
			Columns:      tablestruct,
			Dependencies: dependencies,
			Type:         typen,
		})
	}

	return tables, nil
}

// queryTableDefinition queries the table definition of the given table and returns a list of Column
func queryTableDefinition(db *sql.DB, database, schema, tableName string) ([]Column, error) {
	query := fmt.Sprintf(sqlQueries["queryColumns"], database, database, schema, tableName)

	rows, err := db.Query(query)
	if err != nil {
		return nil, err
	}

	var Columns []Column
	for rows.Next() {
		var col Column
		if err := rows.Scan(&col.Name, &col.Type_Name, &col.Max_Length, &col.Precision, &col.Scale, &col.Collation_Name, &col.Is_Nullable, &col.Is_Identity); err != nil {
			return nil, err
		}
		Columns = append(Columns, col)
	}
	return Columns, nil
}

// queryViewDefinition queries the view definition of the given view and returns the definition as string
func queryViewDefinition(db *sql.DB, database, schema, tableName string) (string, error) {
	query := fmt.Sprintf(sqlQueries["queryViewDefinition"], database, database, schema, tableName)
	row := db.QueryRow(query)

	var definition string
	if err := row.Scan(&definition); err != nil {
		return "", err
	}

	return definition, nil
}

// queryTableDependencies queries the dependencies of the given table and returns a list of Dependency
func queryTableDependencies(db *sql.DB, database, schema, tableName string) ([]Dependency, error) {
	query := fmt.Sprintf(sqlQueries["queryTableDependencies"], database, database, schema, tableName)
	rows, err := db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var dependencies []Dependency

	for rows.Next() {
		var refDB, refSchema, refTable string
		if err := rows.Scan(&refDB, &refSchema, &refTable); err != nil {
			return nil, err
		}

		// Filter out empty references (if any)
		if refDB == "" {
			refDB = database
		}
		if refSchema == "" || refTable == "" {
			continue
		}

		dependencies = append(dependencies, Dependency{
			ReferencedDB:     refDB,
			ReferencedSchema: refSchema,
			ReferencedTable:  refTable,
		})
	}

	return dependencies, nil
}
