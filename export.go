package main

import (
	"fmt"
	"os"
	"regexp"
	"strings"
	"time"
)

// cleanFn replaces "/" with "-" in a given string.
func cleanFn(str string) string {
	return strings.ReplaceAll(str, "/", "-")
}

// createDirectory creates a directory if it doesn't exist.
func createDirectory(dir string) {
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		fmt.Println("creating", dir, "dir.")
		os.MkdirAll(dir, 0777)
	}
}

// writeSQLFile writes the SQL file, info file, and Go struct file for a given view.
func writeSQLFile(dir string, view TableInfo) {
	var err error
	if len(view.Definition) > 10 {
		infofile := generateInfoFile(view)
		if len(view.Definition) > 10 {
			err = os.WriteFile(dir+view.TableName+".sql", []byte(view.Definition), 0644)
			if err != nil {
				fmt.Println("Error writing SQL file:", err)
			}
		}
		if len(infofile) > 10 {
			err = os.WriteFile(dir+view.TableName+".info.md", []byte(infofile), 0644)
			if err != nil {
				fmt.Println("Error writing info file:", err)
			}
		}
	} else if len(view.Database) > 1 {
		infofile := generateTableInfoFile(view)
		if len(infofile) > 10 {
			err := os.WriteFile(dir+cleanFn(view.TableName)+".info.md", []byte(infofile), 0644)
			if err != nil {
				fmt.Println("Error writing info file:", err)
			}
		}
	}
	structFile := generateGoStruct(view)
	if len(structFile) > 10 {
		err = os.WriteFile(dir+view.TableName+".go", []byte(structFile), 0644)
		if err != nil {
			fmt.Println("Error writing Go struct file:", err)
		}
	}
}

// extractDataFromComment extracts creator, creation date, and comment from a given comment string.
func extractDataFromComment(comment string) (string, time.Time, string) {
	creatorRegex := regexp.MustCompile(`(?i)Ersteller/in: (.*)`)
	creationDateRegex := regexp.MustCompile(`(?i)Erstelldatum: (.*)`)
	commentRegex := regexp.MustCompile(`(?i)(Kommentar|Description): (.*)`)

	if !creatorRegex.MatchString(comment) || !creationDateRegex.MatchString(comment) || !commentRegex.MatchString(comment) {
		return "", time.Time{}, ""
	}

	creator := creatorRegex.FindStringSubmatch(comment)[1]
	creationDate := creationDateRegex.FindStringSubmatch(comment)[1]
	commentText := commentRegex.FindStringSubmatch(comment)[2]

	creationDateTime, _ := time.Parse("2006-01-02", creationDate)

	return strings.TrimSpace(creator), creationDateTime, strings.TrimSpace(commentText)
}

// generateInfoFile generates an information file for a given view.
func generateInfoFile(view TableInfo) string {
	sqllines := strings.Split(view.Definition, "\n")
	_, _, commentText := extractDataFromComment(view.Definition)

	infofile := "# Infodatei zum View [" + strings.ToLower(view.Database+"."+view.Schema) + "." + view.TableName + "](../../" + strings.ToLower(view.Database+"/"+view.Schema) + "/" + view.TableName + ".sql)\n\n" + commentText + "\n\n"
	infofile += "## Tabellenstruktur\n\n" + generateTableStructTable(view) + "\n\n## Änderungen\n\nBenutzer|Datum|Kommentar\n--|--|--\n"

	for _, l := range sqllines {
		l = strings.TrimSpace(l)
		if strings.HasPrefix(l, "Commit;") {
			l = strings.TrimPrefix(l, "Commit;")
			infofile += strings.ReplaceAll(l, ";", "|") + "\n"
		}
	}
	infofile += "\n" + "## Abhängigkeiten" + "\n\n" + "DB|Schema|Tabelle/View" + "\n" + "--|--|--" + "\n"
	for _, dep := range view.Dependencies {
		infofile += strings.ToLower(dep.ReferencedDB+"|"+dep.ReferencedSchema+"|["+dep.ReferencedTable+"](../../"+dep.ReferencedDB+"/"+dep.ReferencedSchema) + "/" + dep.ReferencedTable + ".info.md)\n"
	}

	infofile += "\n\n"
	return infofile
}

// generateTableStructTable generates a markdown-table containing the table's structure.
func generateTableStructTable(view TableInfo) string {
	table := "Name|Type|Length|Precision|Scale|Collation|Nullable|Identity\n--|--|--|--|--|--|--|--\n"
	var b2i = map[bool]int8{false: 0, true: 1}

	for _, col := range view.Columns {
		table += fmt.Sprintf("%s|%s|%d|%d|%d|%s|%d|%d\n", col.Name, col.Type_Name, col.Max_Length, col.Precision, col.Scale, col.Collation_Name, b2i[col.Is_Nullable], b2i[col.Is_Identity])
	}
	return table
}

// generateTableInfoFile generates an information file for the given table.
func generateTableInfoFile(view TableInfo) string {
	infofile := "# Infodatei zur Tabelle " + strings.ToLower(view.Database+"."+view.Schema) + "." + view.TableName + "\n\n"
	infofile += "## Tabellenstruktur\n\n" + generateTableStructTable(view) + view.Definition
	return infofile
}

// generateGoStruct generates a Go struct for the given table or view.
func generateGoStruct(view TableInfo) string {
	structDef := "package main\n\n"
	structDef += fmt.Sprintf("// %s represents a database table/view structure\n", view.TableName)
	structDef += fmt.Sprintf("type %s struct {\n", view.TableName)

	for _, col := range view.Columns {
		structDef += fmt.Sprintf("\t%s %s `json:\"%s\"`\n", col.Name, mapSQLTypeToGoType(col.Type_Name), col.Name)
	}
	structDef += "}\n"
	return structDef
}

// mapSQLTypeToGoType maps SQL types to Go types.
func mapSQLTypeToGoType(sqlType string) string {
	typeMap := map[string]string{
		"int":        "int",
		"varchar":    "string",
		"nvarchar":   "string",
		"datetime":   "time.Time",
		"bit":        "bool",
		"float":      "float64",
		"decimal":    "float64",
		// Add more SQL to Go type mappings as needed
	}

	if goType, found := typeMap[sqlType]; found {
		return goType
	}
	return "interface{}"
}

// exportToFiles exports the given list of TableInfo to files.
func exportToFiles(j []TableInfo) error {
	workingDir, err := os.Getwd()
	if err != nil {
		fmt.Printf("Fehler beim Ermitteln des aktuellen Verzeichnisses: %v\n", err)
		return err
	}

	for _, view := range j {
		dir := fmt.Sprintf("%s/vcs/%s/%s/", workingDir, strings.ToLower(view.Database), strings.ToLower(view.Schema))
		if view.Database == "." || len(view.Database) < 2 {
			continue
		}
		createDirectory(dir)
		writeSQLFile(dir, view)
	}
	return nil
}
