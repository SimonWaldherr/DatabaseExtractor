package main

import (
	"encoding/json"
	"encoding/xml"
	"flag"
	"log"
	"os"

	"gopkg.in/yaml.v2"
)

// Config structure for database configurations
type Config struct {
	Server    string   `yaml:"server"`
	User      string   `yaml:"user"`
	Password  string   `yaml:"password"`
	DBType    string   `yaml:"dbtype"`
	Databases []string `yaml:"databases"`
	IncludeTables []string `yaml:"include_tables"`
	ExcludeTables []string `yaml:"exclude_tables"`
}

func main() {
	configFile := flag.String("config", "config.yaml", "Path to configuration file")
	outputType := flag.String("output", "debug", "output type [json,xml,files,debug]")
	useCached := flag.Bool("cached", false, "use cached data")
	templateFile := flag.String("template", "", "Path to custom template file")
	flag.Parse()

	config, err := loadConfig(*configFile)
	if err != nil {
		log.Fatalf("Error loading config: %v", err)
	}

	var data []TableInfo

	if *useCached {
		log.Println("Using cached data")
		data, err = parseCachedData()
	} else {
		log.Println("Querying databases")
		data, err = queryDatabases(config)
	}

	if err != nil {
		log.Fatalf("Error querying databases: %v", err)
	}

	// Filter data based on include/exclude lists
	data = filterData(data, config.IncludeTables, config.ExcludeTables)

	switch *outputType {
	case "json":
		if err = writeToFile("data.json", data, json.Marshal); err != nil {
			log.Printf("Error writing JSON file: %v", err)
		}
	case "xml":
		if err = writeToFile("data.xml", data, xml.Marshal); err != nil {
			log.Printf("Error writing XML file: %v", err)
		}
	case "files":
		if err = exportToFiles(data, *templateFile); err != nil {
			log.Printf("Error writing files: %v", err)
		}
	case "debug":
		log.Printf("Data: %v", data)
	default:
		log.Printf("Unknown output type: %s", *outputType)
	}
}

// filterData filters the TableInfo data based on include and exclude lists
func filterData(data []TableInfo, includeTables, excludeTables []string) []TableInfo {
	includes := make(map[string]bool)
	for _, table := range includeTables {
		includes[table] = true
	}
	excludes := make(map[string]bool)
	for _, table := range excludeTables {
		excludes[table] = true
	}

	var filteredData []TableInfo
	for _, table := range data {
		if len(includes) > 0 && !includes[table.TableName] {
			continue
		}
		if excludes[table.TableName] {
			continue
		}
		filteredData = append(filteredData, table)
	}
	return filteredData
}

// parseCachedData parses cached data from json file to TableInfo slice
func parseCachedData() ([]TableInfo, error) {
	var data []TableInfo

	bytes, err := os.ReadFile("data.json")
	if err != nil {
		return data, err
	}

	err = json.Unmarshal(bytes, &data)
	if err != nil {
		return data, err
	}

	return data, nil
}

func loadConfig(filePath string) (Config, error) {
	var config Config

	data, err := os.ReadFile(filePath)
	if err != nil {
		return config, err
	}

	err = yaml.Unmarshal(data, &config)
	if err != nil {
		return config, err
	}

	return config, nil
}

func writeToFile(filename string, data interface{}, marshalFunc func(interface{}) ([]byte, error)) error {
	bytes, err := marshalFunc(data)
	if err != nil {
		return err
	}
	return os.WriteFile(filename, bytes, 0644)
}
