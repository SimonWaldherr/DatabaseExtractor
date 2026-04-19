package main

import (
"database/sql"
"fmt"

_ "github.com/denisenkom/go-mssqldb"
_ "github.com/go-sql-driver/mysql"
_ "github.com/lib/pq"
_ "modernc.org/sqlite"
)

// TableInfo represents a table
type TableInfo struct {
Database     string
Schema       string
TableName    string
Definition   string
Columns      []Column
Dependencies []Dependency
Type         string
}

// Dependency represents a dependency of a table
type Dependency struct {
ReferencedDB     string
ReferencedSchema string
ReferencedTable  string
}

// Column represents a column in a table
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

// Database is a map with the database name as key and a list of tables as value
type Database map[string][]TableInfo

// typeMap maps the type names from the database to the type names used in the information file
var typeMap = map[string]string{
"BASE TABLE": "Table",
"VIEW":       "View",
"FUNCTION":   "Function",
"PROCEDURE":  "Procedure",
}

// queryDatabases queries the given databases and returns a list of TableInfo.
// It selects the appropriate dialect based on config.Driver.
func queryDatabases(config Config) ([]TableInfo, error) {
dialect := getDialect(config.Driver)

if dialect.NeedsPerDBConn() {
// PostgreSQL and SQLite require a separate connection per database.
var results []TableInfo
for _, database := range config.Databases {
fmt.Printf("Database %s: \n", database)
db, err := sql.Open(dialect.DriverName(), dialect.DSN(config, database))
if err != nil {
return nil, err
}
tables, err := queryTables(db, dialect, database)
db.Close()
if err != nil {
return nil, err
}
results = append(results, tables...)
}
return results, nil
}

// MSSQL and MySQL use a single server-level connection.
db, err := sql.Open(dialect.DriverName(), dialect.DSN(config, ""))
if err != nil {
return nil, err
}
defer db.Close()

var results []TableInfo
for _, database := range config.Databases {
fmt.Printf("Database %s: \n", database)
tables, err := queryTables(db, dialect, database)
if err != nil {
return nil, err
}
results = append(results, tables...)
}

return results, nil
}

// queryTables queries the tables of the given database and returns a list of TableInfo
func queryTables(db *sql.DB, dialect Dialect, database string) ([]TableInfo, error) {
q, args := dialect.QueryTablesSQL(database)
	rows, err := db.Query(q, args...)
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

// Normalise dialect-specific type strings to the shared typeMap keys.
normType := dialect.NormaliseType(typen)
fmt.Printf("Database %s, Schema: %s, %s: %s \n", database, schema, typeMap[normType], tableName)

definition, err := queryViewDefinition(db, dialect, database, schema, tableName)
if err != nil {
return nil, err
}

tablestruct, err := queryTableDefinition(db, dialect, database, schema, tableName)
if err != nil {
return nil, err
}

dependencies, err := queryTableDependencies(db, dialect, database, schema, tableName)
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
Type:         normType,
})
}

return tables, nil
}

// queryTableDefinition queries the column metadata for the given object and
// returns a list of Column using the dialect's scanner.
func queryTableDefinition(db *sql.DB, dialect Dialect, database, schema, tableName string) ([]Column, error) {
	query, args := dialect.QueryColumnsSQL(database, schema, tableName)

	rows, err := db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var columns []Column
	for rows.Next() {
		col, err := dialect.ScanColumnRow(rows)
		if err != nil {
			return nil, err
		}
		columns = append(columns, col)
	}
	return columns, nil
}

// queryViewDefinition queries the DDL/body of the given object and returns it
// as a string (empty for plain tables).
func queryViewDefinition(db *sql.DB, dialect Dialect, database, schema, tableName string) (string, error) {
	query, args := dialect.QueryViewDefinitionSQL(database, schema, tableName)
	row := db.QueryRow(query, args...)
	return dialect.ScanViewDefinition(row)
}

// queryTableDependencies queries the foreign-key dependencies of the given
// object and returns a list of Dependency using the dialect's scanner.
func queryTableDependencies(db *sql.DB, dialect Dialect, database, schema, tableName string) ([]Dependency, error) {
	query, args := dialect.QueryDependenciesSQL(database, schema, tableName)
	rows, err := db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var dependencies []Dependency

	for rows.Next() {
		dep, err := dialect.ScanDependencyRow(rows)
		if err != nil {
			return nil, err
		}

		// Fill in the current database when the reference omits it.
		if dep.ReferencedDB == "" {
			dep.ReferencedDB = database
		}
		if dep.ReferencedSchema == "" || dep.ReferencedTable == "" {
			continue
		}

		dependencies = append(dependencies, dep)
	}

	return dependencies, nil
}
