package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/couchbase/gocb/v2"
	"gopkg.in/yaml.v2"
)

type Credentials struct {
	CBHost       string   `yaml:"cb_host"`
	CBUser       string   `yaml:"cb_user"`
	CBPassword   string   `yaml:"cb_password"`
	CBBucket     string   `yaml:"cb_bucket"`
	CBScope      string   `yaml:"cb_scope"`
	CBCollection string   `yaml:"cb_collection"`
	Targets      []string `yaml:"targets"`
}

var (
	once          sync.Once
	myCredentials Credentials
)

// GetCBCredentials loads Couchbase credentials from a YAML file specified by the CREDENTIALS_FILE environment variable.
// It uses sync.Once to ensure credentials are loaded only once per process.
func GetCBCredentials() (Credentials, error) {
	var credErr error
	once.Do(func() {
		credentialsPath := os.Getenv("CREDENTIALS_FILE")
		if credentialsPath == "" {
			log.Printf("CREDENTIALS_FILE environment variable not set - should contain the path to the credentials.yaml file")
			credErr = fmt.Errorf("CREDENTIALS_FILE environment variable not set")
			return
		}
		if _, err := os.Stat(credentialsPath); err == nil {
			yamlFile, err := os.ReadFile(credentialsPath)
			if err != nil {
				log.Printf("GetCBCredentials: yamlFile.Get err   #%v ", err)
				credErr = err
				return
			}
			err = yaml.Unmarshal(yamlFile, &myCredentials)
			if err != nil {
				log.Printf("GetCBCredentials: Unmarshal: %v", err)
				credErr = err
				return
			}
		} else {
			log.Printf("Credentials file %v not found", credentialsPath)
			credErr = fmt.Errorf("credentials file %v not found", credentialsPath)
			return
		}
	})
	return myCredentials, credErr
}

// GetConnection establishes and returns a Couchbase cluster connection using the provided credentials.
// It applies the WAN development profile and waits for the bucket to be ready.
func GetConnection(credentials Credentials) (*gocb.Cluster, error) {
	host := credentials.CBHost
	if !strings.Contains(host, "couchbase") {
		host = "couchbases://" + host
	}
	username := credentials.CBUser
	password := credentials.CBPassword
	bucketName := credentials.CBBucket
	options := gocb.ClusterOptions{
		Authenticator: gocb.PasswordAuthenticator{
			Username: username,
			Password: password,
		},
	}
	cluster, err := gocb.Connect(host, options)
	if err != nil {
		log.Printf("Failed to connect to Couchbase: %v", err)
		return nil, err
	}
	bucket := cluster.Bucket(bucketName)
	if err := bucket.WaitUntilReady(25*time.Second, nil); err != nil {
		log.Printf("Bucket not ready: %v", err)
		return nil, err
	}
	return cluster, nil
}

// AltimeterToStationPressure converts the altimeter measurement to station pressure.
// altimeterValue: altimeter setting in hPa (millibars)
// height: station elevation in meters
// Returns: station pressure in hPa
// AltimeterToStationPressure converts an altimeter setting (pressure reduced to sea level under standard atmosphere assumptions)
// and station elevation (height above mean sea level in meters) to the actual station pressure.
//
// Convert the altimeter measurement to station pressure.
// This function is useful for working with METARs since they do provide
// altimeter values, but not sea-level pressure or station pressure.
// The following definitions of altimeter setting and station pressure
// are taken from [Smithsonian1951]_ Altimeter setting is the
// pressure value to which an aircraft altimeter scale is set so that it will
// indicate the altitude above mean sea-level of an aircraft on the ground at the
// location for which the value is determined. It assumes a standard atmosphere [NOAA1976]_.
// Station pressure is the atmospheric pressure at the designated station elevation.
// Finding the station pressure can be helpful for calculating sea-level pressure
// or other parameters.
// Parameters:
//   - altimeterValue: The altimeter setting in hectopascals (hPa), typically reported by weather stations.
//   - height: The elevation of the station above mean sea level in meters.
//
// Returns:
//   - The calculated station pressure in hectopascals (hPa).
func AltimeterToStationPressure(altimeterValueHpa, heightMeters float64) float64 {

	// Constants
	const (
		p0    = 1013.25  // standard sea-level pressure in hPa
		t0    = 288.15    // standard sea-level temperature in K
		gamma = 0.0065   // lapse rate in K/m
		Rd    = 287.05   // gas constant for dry air in J/(kg·K)
		g     = 9.80665  // gravity in m/s^2
		n     = 0.190284 // (Rd * gamma / g)
	)

	// Convert altimeter value to hPa if needed (assume input is hPa)
	Amb := altimeterValueHpa

	// Calculate station pressure
	term := math.Pow(Amb, n) - ((math.Pow(p0, n) * gamma * heightMeters) / t0)
	stationPressure := math.Pow(term, 1.0/n) + 0.3

	return stationPressure
}

// main is the entry point of the obs_altitude_to_surface utility. It parses command-line flags for
// the start and end epochs and the output directory, validates the provided arguments, checks that
// the output directory exists and is writable, and then invokes the run function to perform the main
// processing logic.
//
// Required flags:
//
//	--start_epoch   The starting epoch (must be a non-zero integer).
//	--end_epoch     The ending epoch (must be a non-zero integer, greater than start_epoch).
//	--output_dir    The output directory path (must exist and be writable).
//
// The program will exit with an error message if any required flag is missing, if the output
// directory does not exist or is not writable, or if start_epoch is not less than end_epoch.
//
// Example usage:
//
//	obs_altitude_to_surface --start_epoch=1717000000 --end_epoch=1717003600 --output_dir=/tmp/output
func main() {
	startEpoch := flag.Int64("start_epoch", 0, "Start epoch (required)")
	endEpoch := flag.Int64("end_epoch", 0, "End epoch (required)")
	outputDir := flag.String("output_dir", "", "Output directory (required)")
	flag.Parse()
	// Check that outputDir exists and is a writable directory
	info, err := os.Stat(*outputDir)
	if err != nil {
		fmt.Printf("Output directory %s does not exist: %v\n", *outputDir, err)
		os.Exit(1)
	}
	if !info.IsDir() {
		fmt.Printf("Output path %s is not a directory\n", *outputDir)
		os.Exit(1)
	}
	// Try to create a temporary file to check writability
	testFile := fmt.Sprintf("%s/.writetest", *outputDir)
	f, err := os.Create(testFile)
	if err != nil {
		fmt.Printf("Output directory %s is not writable: %v\n", *outputDir, err)
		os.Exit(1)
	}
	f.Close()
	os.Remove(testFile)

	if *startEpoch == 0 || *endEpoch == 0 || *startEpoch >= *endEpoch || *outputDir == "" {
		fmt.Println("Both --start_epoch and --end_epoch must be provided, must be non-zero, start_epoch needs to be less than end_epoch, and --output_dir must be provided.")
		os.Exit(1)
	}

	if err := run(*startEpoch, *endEpoch, *outputDir); err != nil {
		log.Printf("Error: %v", err)
		os.Exit(1)
	}
}

func run(startEpoch, endEpoch int64, outputDir string) error {
	credentials, err := GetCBCredentials()
	stations := make(map[string]interface{})
	if err != nil {
		return fmt.Errorf("failed to get Couchbase credentials: %w", err)
	}
	cluster, err := GetConnection(credentials)
	if err != nil {
		return fmt.Errorf("failed to connect to Couchbase: %w", err)
	}
	defer cluster.Close(nil)
	// Perform the stationQuery and put the results into the stations map
	stationQuery := fmt.Sprintf(
		"SELECT METAR.name, METAR.geo FROM `%s`.`%s`.`%s` WHERE type='MD' AND version='V01' AND subset='METAR' AND docType='station'",
		credentials.CBBucket, credentials.CBScope, credentials.CBCollection,
	)
	stationResults, err := cluster.Query(stationQuery, &gocb.QueryOptions{})
	if err != nil {
		return fmt.Errorf("query for station documents failed: %w", err)
	}
	for stationResults.Next() {
		var row struct {
			Name string      `json:"name"`
			Geo  interface{} `json:"geo"`
		}
		if err := stationResults.Row(&row); err != nil {
			log.Printf("Failed to read station query row: %v", err)
			continue
		}
		stations[row.Name] = row.Geo
	}
	if err := stationResults.Err(); err != nil {
		return fmt.Errorf("error iterating station query results: %w", err)
	}

	query := fmt.Sprintf("SELECT META().id FROM `%s`.`%s`.`%s` WHERE type='DD' AND docType='obs' AND subset='METAR' AND version='V01' AND fcstValidEpoch >= %d AND fcstValidEpoch <= %d AND dataVersion IS MISSING",
		credentials.CBBucket, credentials.CBScope, credentials.CBCollection, startEpoch, endEpoch)
	results, err := cluster.Query(query, &gocb.QueryOptions{})
	if err != nil {
		return fmt.Errorf("query for obs documents failed: %w", err)
	}

	for results.Next() {
		var row struct {
			ID string `json:"id"`
		}
		err := results.Row(&row)
		if err != nil {
			log.Printf("Failed to read query row: %v", err)
			continue
		}
		// Here we fetch the document, process it, and update it.
		collection := cluster.Bucket(credentials.CBBucket).Scope(credentials.CBScope).Collection(credentials.CBCollection)
		getResult, err := collection.Get(row.ID, nil)
		if err != nil {
			log.Printf("Failed to fetch document with ID %s: %v", row.ID, err)
			continue
		}
		var doc map[string]interface{}
		if err := getResult.Content(&doc); err != nil {
			log.Printf("Failed to decode document with ID %s: %v", row.ID, err)
			continue
		}
		data, ok := doc["data"].(map[string]interface{})
		if !ok {
			log.Printf("data field not found or not a map in document with ID %s", row.ID)
			continue
		}
		if doc["dataVersion"] != nil {
			// Skip documents that already have dataVersion
			continue
		}

		for station := range data {
			stationData, ok := data[station].(map[string]interface{})
			if !ok {
				log.Printf("station data not a map for station %s in document with ID %s", station, row.ID)
				continue
			}
			surfacePressure, ok := stationData["Surface Pressure"].(float64)
			if !ok {
				//log.Printf("Surface Pressure not found in document with ID %s and station name %s", row.ID, station)
				continue
			}
			geo, exists := stations[station]
			if !exists {
				log.Printf("No geo information found for station %s", station)
				continue
			}
			geoArr, ok := geo.([]interface{})
			if !ok || len(geoArr) == 0 {
				log.Printf("geo information not an array or empty for station %s", station)
				continue
			}
			lastGeo, ok := geoArr[len(geoArr)-1].(map[string]interface{})
			if !ok {
				log.Printf("last geo entry not a map in station document with name %s", station)
				continue
			}
			elev, ok := lastGeo["elev"].(float64)
			if !ok {
				log.Printf("elev field missing or not a float in station document with name %s", station)
				continue
			}
			stationPressure := AltimeterToStationPressure(surfacePressure, elev)
			doc["data"].(map[string]interface{})[station].(map[string]interface{})["Surface Pressure"] = stationPressure
			doc["data"].(map[string]interface{})[station].(map[string]interface{})["Altimeter Pressure"] = surfacePressure
		}
		doc["dataVersion"] = "1.0.1"

		// Write the updated document to a file in the outputDir named after the row.ID
		outputPath := fmt.Sprintf("%s/%s.json", outputDir, row.ID)
		file, err := os.Create(outputPath)
		if err != nil {
			log.Printf("Failed to create file %s: %v", outputPath, err)
			continue
		}
		encoder := json.NewEncoder(file)
		encoder.SetIndent("", "  ")
		if err := encoder.Encode(doc); err != nil {
			log.Printf("Failed to write document to file %s: %v", outputPath, err)
			file.Close()
			continue
		}
		file.Close()
		log.Printf("Wrote updated document to %s", outputPath)
	}
	return nil
}
