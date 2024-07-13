package main

import (
	"database/sql"
	"fmt"
	"log"
	"sync"

	_ "github.com/denisenkom/go-mssqldb"
	_ "github.com/mattn/go-sqlite3"
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

var sqliteQueries = map[string]string{
	"queryTables":            "SELECT name, type FROM sqlite_master WHERE type IN ('table', 'view')",
	"queryColumns":           "PRAGMA table_info(%s)",
	"queryViewDefinition":    "SELECT sql FROM sqlite_master WHERE name='%s' AND type='view'",
	"queryTableDependencies": "SELECT '' as referenced_database_name, '' as referenced_schema_name, '' as referenced_entity_name", // SQLite doesn't support this
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
	var wg sync.WaitGroup
	results := make(chan TableInfo)
	errors := make(chan error)
	done := make(chan bool)

	for _, database := range config.Databases {
		wg.Add(1)
		go func(dbName string) {
			defer wg.Done()
			tables, err := queryTables(config.Server, dbName, config)
			if err != nil {
				errors <- err
				return
			}
			for _, table := range tables {
				results <- table
			}
		}(database)
	}

	go func() {
		wg.Wait()
		close(results)
		close(errors)
		done <- true
	}()

	var tableInfos []TableInfo
	var errs []error

	for {
		select {
		case table := <-results:
			tableInfos = append(tableInfos, table)
		case err := <-errors:
			errs = append(errs, err)
		case <-done:
			if len(errs) > 0 {
				return tableInfos, errs[0]
			}
			return tableInfos, nil
		}
	}
}

// queryTables queries the tables of the given database and returns a list of TableInfo
func queryTables(server, database string, config Config) ([]TableInfo, error) {
	var db *sql.DB
	var err error
	var query string

	if config.DBType == "sqlite" {
		db, err = sql.Open("sqlite3", server)
		query = sqliteQueries["queryTables"]
	} else {
		connString := fmt.Sprintf("server=%s;user id=%s;password=%s;port=1433", server, config.User, config.Password)
		db, err = sql.Open("mssql", connString)
		query = fmt.Sprintf(sqlQueries["queryTables"], database, database)
	}

	if err != nil {
		return nil, err
	}
	defer db.Close()

	rows, err := db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tables []TableInfo

	for rows.Next() {
		var tableName, typen string

		if err := rows.Scan(&tableName, &typen); err != nil {
			return nil, err
		}

		log.Printf("Database %s, %s: %s \n", database, typeMap[typen], tableName)

		definition, err := queryViewDefinition(db, database, tableName, config)
		if err != nil {
			return nil, err
		}

		tablestruct, err := queryTableDefinition(db, database, tableName, config)
		if err != nil {
			return nil, err
		}

		dependencies, err := queryTableDependencies(db, database, tableName, config)
		if err != nil {
			return nil, err
		}

		tables = append(tables, TableInfo{
			Database:     database,
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
func queryTableDefinition(db *sql.DB, database, tableName string, config Config) ([]Column, error) {
	var query string

	if config.DBType == "sqlite" {
		query = fmt.Sprintf(sqliteQueries["queryColumns"], tableName)
	} else {
		query = fmt.Sprintf(sqlQueries["queryColumns"], database, database, "", tableName)
	}

	rows, err := db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var columns []Column
	for rows.Next() {
		var col Column
		if config.DBType == "sqlite" {
			var cid, notnull, dfltValue, pk int
			if err := rows.Scan(&cid, &col.Name, &col.Type_Name, &notnull, &dfltValue, &pk); err != nil {
				return nil, err
			}
			col.Is_Nullable = notnull == 0
		} else {
			if err := rows.Scan(&col.Name, &col.Type_Name, &col.Max_Length, &col.Precision, &col.Scale, &col.Collation_Name, &col.Is_Nullable, &col.Is_Identity); err != nil {
				return nil, err
			}
		}
		columns = append(columns, col)
	}
	return columns, nil
}

// queryViewDefinition queries the view definition of the given view and returns the definition as string
func queryViewDefinition(db *sql.DB, database, tableName string, config Config) (string, error) {
	var query string

	if config.DBType == "sqlite" {
		query = fmt.Sprintf(sqliteQueries["queryViewDefinition"], tableName)
	} else {
		query = fmt.Sprintf(sqlQueries["queryViewDefinition"], database, database, "", tableName)
	}

	row := db.QueryRow(query)

	var definition string
	if err := row.Scan(&definition); err != nil {
		return "", err
	}

	return definition, nil
}

// queryTableDependencies queries the dependencies of the given table and returns a list of Dependency
func queryTableDependencies(db *sql.DB, database, tableName string, config Config) ([]Dependency, error) {
	var query string

	if config.DBType == "sqlite" {
		query = sqliteQueries["queryTableDependencies"]
	} else {
		query = fmt.Sprintf(sqlQueries["queryTableDependencies"], database, database, "", tableName)
	}

	rows, err := db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var dependencies []Dependency

	for rows.Next() {
		var dep Dependency
		if err := rows.Scan(&dep.ReferencedDB, &dep.ReferencedSchema, &dep.ReferencedTable); err != nil {
			return nil, err
		}
		dependencies = append(dependencies, dep)
	}

	return dependencies, nil
}
