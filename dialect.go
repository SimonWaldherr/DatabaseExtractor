package main

import (
	"database/sql"
	"fmt"
	"strings"
)

// Dialect abstracts DBMS-specific connection and query behaviour.
type Dialect interface {
	// DriverName returns the name registered with database/sql.
	DriverName() string
	// DSN builds the data-source name for a given database within a config.
	// For server-level dialects (MSSQL, MySQL) database may be empty.
	DSN(config Config, database string) string
	// NeedsPerDBConn returns true when the dialect requires a separate
	// connection for every logical database (PostgreSQL, SQLite).
	NeedsPerDBConn() bool

	// QueryTablesSQL returns a SQL statement that yields four columns:
	// catalog, schema, name, type for every table / view / routine.
	QueryTablesSQL(database string) string
	// NormaliseType maps the raw type string returned by the DBMS to one of
	// the keys understood by the typeMap in database.go.
	NormaliseType(raw string) string

	// QueryColumnsSQL returns a SQL statement that yields the column
	// metadata for a specific object.
	QueryColumnsSQL(database, schema, table string) string
	// ScanColumnRow scans one row from the columns result-set.
	ScanColumnRow(rows *sql.Rows) (Column, error)

	// QueryViewDefinitionSQL returns a SQL statement that yields a single
	// string: the DDL/body of the requested object (empty for plain tables).
	QueryViewDefinitionSQL(database, schema, table string) string
	// ScanViewDefinition scans the single row returned by the view
	// definition query.
	ScanViewDefinition(row *sql.Row) (string, error)

	// QueryDependenciesSQL returns a SQL statement that yields the foreign
	// references for an object.
	QueryDependenciesSQL(database, schema, table string) string
	// ScanDependencyRow scans one row from the dependencies result-set.
	ScanDependencyRow(rows *sql.Rows) (Dependency, error)
}

// getDialect returns the Dialect implementation for the given driver name.
// Defaults to MSSQL for backwards-compatibility when driver is empty.
func getDialect(driver string) Dialect {
	switch strings.ToLower(driver) {
	case "mysql", "mariadb":
		return mysqlDialect{}
	case "postgres", "postgresql":
		return postgresDialect{}
	case "sqlite", "sqlite3":
		return sqliteDialect{}
	default:
		return mssqlDialect{}
	}
}

// ---------------------------------------------------------------------------
// MSSQL dialect
// ---------------------------------------------------------------------------

type mssqlDialect struct{}

func (mssqlDialect) DriverName() string { return "mssql" }

func (mssqlDialect) DSN(config Config, _ string) string {
	port := config.Port
	if port == 0 {
		port = 1433
	}
	return fmt.Sprintf("server=%s;user id=%s;password=%s;port=%d",
		config.Server, config.User, config.Password, port)
}

func (mssqlDialect) NeedsPerDBConn() bool { return false }

func (mssqlDialect) QueryTablesSQL(database string) string {
	return fmt.Sprintf(
		"SELECT TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE "+
			"FROM [%s].INFORMATION_SCHEMA.TABLES "+
			"UNION ALL "+
			"SELECT ROUTINE_CATALOG, ROUTINE_SCHEMA, ROUTINE_NAME, ROUTINE_TYPE "+
			"FROM [%s].INFORMATION_SCHEMA.ROUTINES",
		database, database)
}

func (mssqlDialect) NormaliseType(raw string) string { return raw }

func (mssqlDialect) QueryColumnsSQL(database, schema, table string) string {
	return fmt.Sprintf(
		"USE %s; SELECT c.Name, [Type_Name] = tp.name, c.Max_Length, c.[Precision], "+
			"c.Scale, ISNULL(c.Collation_Name, '') as Collation_Name, c.Is_Nullable, c.Is_Identity "+
			"FROM sys.columns c WITH(NOLOCK) "+
			"JOIN sys.types tp WITH(NOLOCK) ON c.user_type_id = tp.user_type_id "+
			"WHERE c.[object_id] = OBJECT_ID(N'[%s].[%s].[%s]')",
		database, database, schema, table)
}

func (mssqlDialect) ScanColumnRow(rows *sql.Rows) (Column, error) {
	var col Column
	err := rows.Scan(&col.Name, &col.Type_Name, &col.Max_Length, &col.Precision,
		&col.Scale, &col.Collation_Name, &col.Is_Nullable, &col.Is_Identity)
	return col, err
}

func (mssqlDialect) QueryViewDefinitionSQL(database, schema, table string) string {
	return fmt.Sprintf(
		"USE %s; SELECT ISNULL(OBJECT_DEFINITION(OBJECT_ID(N'[%s].[%s].[%s]')),'') as [definition]",
		database, database, schema, table)
}

func (mssqlDialect) ScanViewDefinition(row *sql.Row) (string, error) {
	var definition string
	return definition, row.Scan(&definition)
}

func (mssqlDialect) QueryDependenciesSQL(database, schema, table string) string {
	return fmt.Sprintf(
		"SELECT ISNULL(referenced_database_name, '') as referenced_database_name, "+
			"ISNULL(referenced_schema_name,'') as referenced_schema_name, "+
			"ISNULL(referenced_entity_name,'') as referenced_entity_name "+
			"FROM [%s].sys.sql_expression_dependencies "+
			"WHERE referencing_id = OBJECT_ID(N'[%s].[%s].[%s]')",
		database, database, schema, table)
}

func (mssqlDialect) ScanDependencyRow(rows *sql.Rows) (Dependency, error) {
	var dep Dependency
	err := rows.Scan(&dep.ReferencedDB, &dep.ReferencedSchema, &dep.ReferencedTable)
	return dep, err
}

// ---------------------------------------------------------------------------
// MySQL / MariaDB dialect
// ---------------------------------------------------------------------------

type mysqlDialect struct{}

func (mysqlDialect) DriverName() string { return "mysql" }

func (mysqlDialect) DSN(config Config, _ string) string {
	port := config.Port
	if port == 0 {
		port = 3306
	}
	return fmt.Sprintf("%s:%s@tcp(%s:%d)/",
		config.User, config.Password, config.Server, port)
}

func (mysqlDialect) NeedsPerDBConn() bool { return false }

func (mysqlDialect) QueryTablesSQL(database string) string {
	return fmt.Sprintf(
		"SELECT TABLE_SCHEMA, TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE "+
			"FROM information_schema.TABLES "+
			"WHERE TABLE_SCHEMA = '%s' "+
			"UNION ALL "+
			"SELECT ROUTINE_SCHEMA, ROUTINE_SCHEMA, ROUTINE_NAME, ROUTINE_TYPE "+
			"FROM information_schema.ROUTINES "+
			"WHERE ROUTINE_SCHEMA = '%s'",
		database, database)
}

func (mysqlDialect) NormaliseType(raw string) string { return raw }

func (mysqlDialect) QueryColumnsSQL(database, _, table string) string {
	return fmt.Sprintf(
		"SELECT COLUMN_NAME, DATA_TYPE, "+
			"COALESCE(CHARACTER_MAXIMUM_LENGTH, 0), "+
			"COALESCE(NUMERIC_PRECISION, 0), "+
			"COALESCE(NUMERIC_SCALE, 0), "+
			"COALESCE(COLLATION_NAME, ''), "+
			"IF(IS_NULLABLE='YES', true, false), "+
			"IF(EXTRA='auto_increment', true, false) "+
			"FROM information_schema.COLUMNS "+
			"WHERE TABLE_SCHEMA = '%s' AND TABLE_NAME = '%s'",
		database, table)
}

func (mysqlDialect) ScanColumnRow(rows *sql.Rows) (Column, error) {
	var col Column
	err := rows.Scan(&col.Name, &col.Type_Name, &col.Max_Length, &col.Precision,
		&col.Scale, &col.Collation_Name, &col.Is_Nullable, &col.Is_Identity)
	return col, err
}

func (mysqlDialect) QueryViewDefinitionSQL(database, _, table string) string {
	return fmt.Sprintf(
		"SELECT COALESCE(VIEW_DEFINITION, '') "+
			"FROM information_schema.VIEWS "+
			"WHERE TABLE_SCHEMA = '%s' AND TABLE_NAME = '%s'",
		database, table)
}

func (mysqlDialect) ScanViewDefinition(row *sql.Row) (string, error) {
	var definition string
	err := row.Scan(&definition)
	// A table (not a view) won't have a row in information_schema.VIEWS.
	if err == sql.ErrNoRows {
		return "", nil
	}
	return definition, err
}

func (mysqlDialect) QueryDependenciesSQL(database, _, table string) string {
	return fmt.Sprintf(
		"SELECT COALESCE(REFERENCED_TABLE_SCHEMA, ''), "+
			"COALESCE(REFERENCED_TABLE_SCHEMA, ''), "+
			"COALESCE(REFERENCED_TABLE_NAME, '') "+
			"FROM information_schema.KEY_COLUMN_USAGE "+
			"WHERE TABLE_SCHEMA = '%s' AND TABLE_NAME = '%s' "+
			"AND REFERENCED_TABLE_NAME IS NOT NULL "+
			"GROUP BY REFERENCED_TABLE_SCHEMA, REFERENCED_TABLE_NAME",
		database, table)
}

func (mysqlDialect) ScanDependencyRow(rows *sql.Rows) (Dependency, error) {
	var dep Dependency
	err := rows.Scan(&dep.ReferencedDB, &dep.ReferencedSchema, &dep.ReferencedTable)
	return dep, err
}

// ---------------------------------------------------------------------------
// PostgreSQL dialect
// ---------------------------------------------------------------------------

type postgresDialect struct{}

func (postgresDialect) DriverName() string { return "postgres" }

func (postgresDialect) DSN(config Config, database string) string {
	port := config.Port
	if port == 0 {
		port = 5432
	}
	return fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		config.Server, port, config.User, config.Password, database)
}

func (postgresDialect) NeedsPerDBConn() bool { return true }

func (postgresDialect) QueryTablesSQL(_ string) string {
	return "SELECT table_catalog, table_schema, table_name, table_type " +
		"FROM information_schema.tables " +
		"WHERE table_schema NOT IN ('pg_catalog', 'information_schema') " +
		"UNION ALL " +
		"SELECT routine_catalog, routine_schema, routine_name, routine_type " +
		"FROM information_schema.routines " +
		"WHERE routine_schema NOT IN ('pg_catalog', 'information_schema')"
}

func (postgresDialect) NormaliseType(raw string) string { return raw }

func (postgresDialect) QueryColumnsSQL(_, schema, table string) string {
	return fmt.Sprintf(
		"SELECT column_name, data_type, "+
			"COALESCE(character_maximum_length, 0), "+
			"COALESCE(numeric_precision, 0), "+
			"COALESCE(numeric_scale, 0), "+
			"COALESCE(collation_name, ''), "+
			"CASE WHEN is_nullable = 'YES' THEN true ELSE false END, "+
			"CASE WHEN column_default LIKE 'nextval%%' THEN true ELSE false END "+
			"FROM information_schema.columns "+
			"WHERE table_schema = '%s' AND table_name = '%s'",
		schema, table)
}

func (postgresDialect) ScanColumnRow(rows *sql.Rows) (Column, error) {
	var col Column
	err := rows.Scan(&col.Name, &col.Type_Name, &col.Max_Length, &col.Precision,
		&col.Scale, &col.Collation_Name, &col.Is_Nullable, &col.Is_Identity)
	return col, err
}

func (postgresDialect) QueryViewDefinitionSQL(_, schema, table string) string {
	return fmt.Sprintf(
		"SELECT COALESCE(view_definition, '') "+
			"FROM information_schema.views "+
			"WHERE table_schema = '%s' AND table_name = '%s'",
		schema, table)
}

func (postgresDialect) ScanViewDefinition(row *sql.Row) (string, error) {
	var definition string
	err := row.Scan(&definition)
	if err == sql.ErrNoRows {
		return "", nil
	}
	return definition, err
}

func (postgresDialect) QueryDependenciesSQL(_, schema, table string) string {
	return fmt.Sprintf(
		"SELECT tc.table_catalog, ccu.table_schema, ccu.table_name "+
			"FROM information_schema.table_constraints AS tc "+
			"JOIN information_schema.constraint_column_usage AS ccu "+
			"ON ccu.constraint_name = tc.constraint_name "+
			"AND ccu.table_schema = tc.table_schema "+
			"WHERE tc.constraint_type = 'FOREIGN KEY' "+
			"AND tc.table_schema = '%s' AND tc.table_name = '%s' "+
			"GROUP BY tc.table_catalog, ccu.table_schema, ccu.table_name",
		schema, table)
}

func (postgresDialect) ScanDependencyRow(rows *sql.Rows) (Dependency, error) {
	var dep Dependency
	err := rows.Scan(&dep.ReferencedDB, &dep.ReferencedSchema, &dep.ReferencedTable)
	return dep, err
}

// ---------------------------------------------------------------------------
// SQLite dialect
// ---------------------------------------------------------------------------

type sqliteDialect struct{}

func (sqliteDialect) DriverName() string { return "sqlite" }

// DSN for SQLite: config.Server is the path to the database file.
// If databases are listed, the first entry is used; otherwise config.Server.
func (sqliteDialect) DSN(config Config, database string) string {
	if database != "" {
		return database
	}
	return config.Server
}

func (sqliteDialect) NeedsPerDBConn() bool { return true }

func (sqliteDialect) QueryTablesSQL(_ string) string {
	return "SELECT '' as catalog, '' as schema_name, name, type " +
		"FROM sqlite_master " +
		"WHERE type IN ('table', 'view') AND name NOT LIKE 'sqlite_%'"
}

// NormaliseType maps SQLite's lowercase type names to the shared typeMap keys.
func (sqliteDialect) NormaliseType(raw string) string {
	switch strings.ToUpper(raw) {
	case "TABLE":
		return "BASE TABLE"
	case "VIEW":
		return "VIEW"
	default:
		return strings.ToUpper(raw)
	}
}

// QueryColumnsSQL returns a PRAGMA statement; SQLite does not use the
// standard information_schema for column metadata.
func (sqliteDialect) QueryColumnsSQL(_, _, table string) string {
	return fmt.Sprintf("PRAGMA table_info(%s)", table)
}

// ScanColumnRow scans a PRAGMA table_info row:
// cid | name | type | notnull | dflt_value | pk
func (sqliteDialect) ScanColumnRow(rows *sql.Rows) (Column, error) {
	var cid, notnull, pk int
	var dfltValue sql.NullString
	var col Column
	err := rows.Scan(&cid, &col.Name, &col.Type_Name, &notnull, &dfltValue, &pk)
	col.Is_Nullable = notnull == 0
	col.Is_Identity = pk != 0
	return col, err
}

func (sqliteDialect) QueryViewDefinitionSQL(_, _, table string) string {
	return fmt.Sprintf(
		"SELECT COALESCE(sql, '') FROM sqlite_master WHERE type='view' AND name='%s'",
		table)
}

func (sqliteDialect) ScanViewDefinition(row *sql.Row) (string, error) {
	var definition string
	err := row.Scan(&definition)
	if err == sql.ErrNoRows {
		return "", nil
	}
	return definition, err
}

// QueryDependenciesSQL returns a PRAGMA statement for foreign keys.
func (sqliteDialect) QueryDependenciesSQL(_, _, table string) string {
	return fmt.Sprintf("PRAGMA foreign_key_list(%s)", table)
}

// ScanDependencyRow scans a PRAGMA foreign_key_list row:
// id | seq | table | from | to | on_update | on_delete | match
func (sqliteDialect) ScanDependencyRow(rows *sql.Rows) (Dependency, error) {
	var id, seq int
	var refTable, fromCol, toCol, onUpdate, onDelete, match string
	err := rows.Scan(&id, &seq, &refTable, &fromCol, &toCol, &onUpdate, &onDelete, &match)
	dep := Dependency{ReferencedTable: refTable}
	return dep, err
}
