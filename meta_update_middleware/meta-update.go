package main

import (
	"encoding/json"
	"flag"
	"log"
	"os"
	"runtime"
	"runtime/pprof"
	"time"

	"gopkg.in/yaml.v3"
	// "github.com/couchbase/gocb/v2"
)

type StrArray []string

type ConfigJSON struct {
	Metadata []struct {
		Name       string   `json:"name"`
		App        string   `json:"app"`
		SubDocType string   `json:"subDocType"`
		DocType    StrArray `json:"docType"`
	} `json:"metadata"`
}

type Credentials struct {
	Cb_host            string `yaml:"cb_host"`
	Cb_user            string `yaml:"cb_user"`
	Cb_password        string `yaml:"cb_password"`
	Cb_bucket          string `yaml:"cb_bucket"`
	Cb_scope           string `yaml:"cb_scope"`
	Cb_collection      string `yaml:"cb_collection"`
	Cb_timeout_seconds int    `yaml:"cb_timeout_seconds"`
}

// init runs before main() is evaluated
func init() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	log.Println("meta-update:init()")
}

func main() {
	// Uncomment following line to enable logging
	// gocb.SetLogger(gocb.VerboseStdioLogger())

	start := time.Now()
	log.Print("meta-update:main()")

	home, _ := os.UserHomeDir()
	credentialsPath := os.Getenv("CREDENTIALS_FILE")
	if credentialsPath == "" {
		credentialsPath = home + "/credentials"
	}
	var credentialsFilePath string
	flag.StringVar(&credentialsFilePath, "c", credentialsPath, "path to credentials file")

	var settingsFilePath string
	flag.StringVar(&settingsFilePath, "s", "./settings.json", "path to settings.json file")

	var app string
	flag.StringVar(&app, "a", "", "app name")

	var cpuProfilePath string
	flag.StringVar(&cpuProfilePath, "cpuprofile", "", "write CPU profile to file")

	var memProfilePath string
	flag.StringVar(&memProfilePath, "memprofile", "", "write heap profile to file")

	var queryMetrics bool
	flag.BoolVar(&queryMetrics, "query-metrics", true, "enable Couchbase query metrics")

	var queryProfile string
	flag.StringVar(&queryProfile, "query-profile", "off", "Couchbase query profiling mode: off|phases|timings")

	var querySlowMs int
	flag.IntVar(&querySlowMs, "query-slow-ms", 500, "log query metadata when elapsed time is at least this many milliseconds; use 0 for all queries")

	var querySummaryTop int
	flag.IntVar(&querySummaryTop, "query-summary-top", 10, "number of slow query templates to include in end-of-run summary; use 0 for all")

	var path string
	flag.StringVar(&path, "p", "", "path to output metadata")

	flag.Parse()

	if len(cpuProfilePath) > 0 {
		f, err := os.Create(cpuProfilePath)
		if err != nil {
			log.Fatalf("unable to create cpu profile file %s: %v", cpuProfilePath, err)
		}
		if err := pprof.StartCPUProfile(f); err != nil {
			f.Close()
			log.Fatalf("unable to start cpu profile: %v", err)
		}
		defer func() {
			pprof.StopCPUProfile()
			if err := f.Close(); err != nil {
				log.Printf("unable to close cpu profile file %s: %v", cpuProfilePath, err)
			}
		}()
	}

	setQueryProfilingOptions(queryMetrics, queryProfile, querySlowMs)

	if len(app) > 0 {
		log.Println("meta-update, settings file:" + settingsFilePath + ",credentials file:" + credentialsFilePath + ",app:" + app)
	} else {
		log.Println("meta-update, settings file:" + settingsFilePath + ",credentials file:" + credentialsFilePath + ",app:[all apps in settings file]")
	}

	conf := ConfigJSON{}

	conf, err := parseConfig(settingsFilePath)
	if err != nil {
		log.Fatal("Unable to parse config")
		return
	}

	credentials := getCredentials(credentialsFilePath)

	conn := getDbConnection(credentials)

	//testGetSingleCTC(conn)
	//testGetCTCCount(connSrc)

	for ds := 0; ds < len(conf.Metadata); ds++ {
		if len(app) > 0 {
			if app != conf.Metadata[ds].Name {
				continue
			}
		}
		for dt := 0; dt < len(conf.Metadata[ds].DocType); dt++ {
			log.Println("Metadata:" + conf.Metadata[ds].Name + ",DocType:" + conf.Metadata[ds].DocType[dt])
			updateMedataForAppDocType(conn, conf.Metadata[ds].Name, conf.Metadata[ds].App, conf.Metadata[ds].DocType[dt], conf.Metadata[ds].SubDocType, path)
		}
	}

	if len(memProfilePath) > 0 {
		f, err := os.Create(memProfilePath)
		if err != nil {
			log.Fatalf("unable to create memory profile file %s: %v", memProfilePath, err)
		}
		runtime.GC()
		if err := pprof.WriteHeapProfile(f); err != nil {
			f.Close()
			log.Fatalf("unable to write memory profile: %v", err)
		}
		if err := f.Close(); err != nil {
			log.Printf("unable to close memory profile file %s: %v", memProfilePath, err)
		}
	}

	printQueryProfilingSummary(querySummaryTop)

	log.Printf("\tmeta update finished in %v\n", time.Since(start))
}

func updateMedataForAppDocType(conn CbConnection, name string, app string, doctype string, subDocType string, path string) {
	log.Println("updateMedataForAppDocType(" + name + "," + doctype + ")")

	// get needed models
	models := getModels(conn, name, app, doctype, subDocType)
	log.Println("models:")
	printStringArray(models)

	// get models having metadata but no data (remove metadata for these)
	// (note 'like %' is changed to 'like %25')
	models_with_metatada_but_no_data := getModelsNoData(conn, name, app, doctype, subDocType)
	log.Println("models_with_metatada_but_no_data:")
	printStringArray(models_with_metatada_but_no_data)

	metadata := MetadataJSON{ID: "MD:matsGui:" + name + ":COMMON:V01", Name: name, App: app, Type: "MD", Version: "V01", Subset: "COMMON", DocType: "matsGui", Generated: true}
	metadata.Updated = 0

	for i, m := range models {
		model := Model{Name: m}
		data_keys := getDistinctDataKeys(conn, name, app, doctype, subDocType, m)
		log.Println(data_keys)
		fcstLen := getDistinctFcstLen(conn, name, app, doctype, subDocType, m)
		log.Println(fcstLen)
		region := getDistinctRegion(conn, name, app, doctype, subDocType, m)
		log.Println(region)
		displayText := getDistinctDisplayText(conn, name, app, doctype, subDocType, m)
		log.Println(displayText)
		displayCategory := getDistinctDisplayCategory(conn, name, app, doctype, subDocType, m)
		log.Println(displayCategory)
		displayOrder := getDistinctDisplayOrder(conn, name, app, doctype, subDocType, m, i)
		log.Println(displayOrder)
		minMaxCountFloor := getMinMaxCountFloor(conn, name, app, doctype, subDocType, m)
		log.Println(jsonPrettyPrintStruct(minMaxCountFloor[0].(map[string]interface{})))

		// ./sqls/getDistinctThresholds.sql returns list of variables for SUMS DocType, like in Surface
		if doctype == "SUMS" {
			model.Variables = data_keys
		} else {
			model.Thresholds = data_keys
		}
		model.Model = models[i]
		model.FcstLens = fcstLen
		model.Regions = region
		model.DisplayText = displayText[0]
		model.DisplayCategory = displayCategory[0]
		model.DisplayOrder = displayOrder[0]
		minMaxRow := minMaxCountFloor[0].(map[string]interface{})
		model.Mindate = getOptionalIntField(minMaxRow, "mindate", 0)
		model.Maxdate = getOptionalIntField(minMaxRow, "maxdate", 0)
		model.Numrecs = getOptionalIntField(minMaxRow, "numrecs", 0)
		metadata.Updated = getOptionalIntField(minMaxRow, "updated", 0)
		metadata.Models = append(metadata.Models, model)
	}
	log.Println(jsonPrettyPrintStruct(metadata))
	writeMetadata(conn, metadata, path)
}

func getOptionalIntField(row map[string]interface{}, key string, defaultValue int) int {
	value, exists := row[key]
	if !exists || value == nil {
		return defaultValue
	}

	switch v := value.(type) {
	case float64:
		return int(v)
	case float32:
		return int(v)
	case int:
		return v
	case int32:
		return int(v)
	case int64:
		return int(v)
	case uint:
		return int(v)
	case uint32:
		return int(v)
	case uint64:
		return int(v)
	default:
		log.Printf("unexpected type for %s: %T; using default %d", key, value, defaultValue)
		return defaultValue
	}
}

func parseConfig(file string) (ConfigJSON, error) {
	log.Println("parseConfig(" + file + ")")

	conf := ConfigJSON{}
	configFile, err := os.Open(file)
	if err != nil {
		log.Print("opening config file", err.Error())
		configFile.Close()
		return conf, err
	}
	defer configFile.Close()

	jsonParser := json.NewDecoder(configFile)
	if err = jsonParser.Decode(&conf); err != nil {
		log.Fatalln("parsing config file", err.Error())
		return conf, err
	}

	return conf, nil
}

func getCredentials(credentialsFilePath string) Credentials {
	creds := Credentials{}
	yamlFile, err := os.ReadFile(credentialsFilePath)
	if err != nil {
		log.Printf("yamlFile.Get err   #%v ", err)
	}
	err = yaml.Unmarshal(yamlFile, &creds)
	if err != nil {
		log.Fatalf("Unmarshal: %v", err)
	}
	return creds
}
