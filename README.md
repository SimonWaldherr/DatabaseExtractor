# Database Extractor
## a Database Structure and Query Export Tool

This is a Go application designed to query tables, views, and their dependencies from a Microsoft SQL Server or SQLite database and export the retrieved structure data into various formats such as JSON, XML, markdown files, and Golang structs.

## Features

- Query tables and views from one or more specified databases.
- Export the queried data to JSON, XML, markdown files, or Go structs.
- Handle dependencies between tables and views.
- Support for SQLite.
- Enhanced error handling and logging.
- Schema visualization with Graphviz or Mermaid.js.
- Advanced filtering options.
- Custom export formats with templates.
- Concurrent querying of multiple databases or tables.

## Prerequisites

- Go programming language installed on your system.
- Access to a Microsoft SQL Server or SQLite database.
- Configuration file (`config.yaml`) specifying the server details, user credentials, and databases to query.

## Installation

1. Clone this repository to your local machine:
2. Navigate to the project directory:
3. Build the application:

```bash
go build
```

## Usage

1. Ensure you have configured the `config.yaml` file with the necessary server details, user credentials, and databases to query.
2. Run the application with appropriate command-line flags:

- `-config`: Path to the configuration file (default: `config.yaml`).
- `-output`: Output type (options: `json`, `xml`, `files`, `debug`).
- `-cached`: Use cached data if available (options: `true`, `false`).
- `-template`: Path to custom template file for exporting data.

3. The application will query the specified databases, export the data based on the output type, and either write it to files or log it for debugging.

## Configuration

The `config.yaml` file contains the following configuration options:

```yaml
server: "mssql.example.tld"
user: "john.doe"
password: "$ecret£assw⌀rd"
dbtype: "mssql"
databases:
  - "Sales"
  - "Purchases"
  - "Customers"
include_tables:
  - "Sales.Orders"
  - "Customers.CustomerDetails"
exclude_tables:
  - "Sales.TempData"
```

- `server`: The address of the Microsoft SQL Server or SQLite file path.
- `user`: Username to authenticate with the server (not needed for SQLite).
- `password`: Password for the user (not needed for SQLite).
- `dbtype`: The type of the database (options: `mssql`, `sqlite`).
- `databases`: List of databases to query.
- `include_tables`: List of tables to include in the export.
- `exclude_tables`: List of tables to exclude from the export.

## Contributing

Contributions are welcome! Please feel free to open issues or submit pull requests to improve this application.
