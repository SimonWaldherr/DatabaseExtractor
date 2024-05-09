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
	Databases []string `yaml:"databases"`
}

func main() {
	configFile := flag.String("config", "config.yaml", "Path to configuration file")
	outputType := flag.String("output", "debug", "output type [json,xml,files,debug]")
	useCached := flag.Bool("cached", false, "use cached data")
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
		if err = exportToFiles(data); err != nil {
			log.Printf("Error writing files: %v", err)
		}
	case "debug":
		log.Printf("Data: %v", data)
	default:
		log.Printf("Unknown output type: %s", *outputType)
	}
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

/*
func marshalToSQL(data interface{}) ([]byte, error) {
	// Implement the logic to convert data to SQL format
	// Placeholder function, needs actual implementation
	return []byte("SQL DATA"), nil
}
*/
