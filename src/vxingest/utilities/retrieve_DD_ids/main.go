package main

import (
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/couchbase/gocb/v2"
	"gopkg.in/yaml.v2"
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

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage: retrieve_DD_ids document_id_prefix [print_ids]")
		os.Exit(1)
	}
	prefix := os.Args[1]
	printIDs := false
	if len(os.Args) > 2 && os.Args[2] == "print_ids" {
		printIDs = true
	}

	credentials, err := GetCBCredentials()
	if err != nil {
		log.Fatalf("failed to get Couchbase credentials: %v", err)
	}
	cluster, err := GetConnection(credentials)
	if err != nil {
		log.Fatalf("failed to connect to Couchbase: %v", err)
	}
	defer cluster.Close(nil)

	collection := cluster.Bucket(credentials.CBBucket).Scope(credentials.CBScope).Collection(credentials.CBCollection)
	GetAllIDsWithPrintOption(collection, prefix, printIDs)
}

// GetAllIDsWithPrintOption is like GetAllIDs but optionally prints IDs to stdout.
func GetAllIDsWithPrintOption(collection *gocb.Collection, prefix string, printIDs bool) {
	scanType := gocb.NewRangeScanForPrefix(prefix)
	opts := &gocb.ScanOptions{
		IDsOnly: true,
	}
	result, err := collection.Scan(scanType, opts)
	if err != nil {
		log.Fatalf("Scan failed: %v", err)
	}
	count := 0
	for {
		item := result.Next()
		if item == nil || item.ID() == "" {
			break
		}
		id := item.ID()
		count++
		if printIDs {
			fmt.Println(id)
		}
		if count%100000 == 0 {
			log.Printf("Processed %d document IDs like %s", count, id)
		}
	}
	log.Printf("Total ids processed: %d", count)
}

func GetAllIDs(collection *gocb.Collection, prefix string) {
	// 1. Define the Scan Type (Range scan for the given prefix)
	scanType := gocb.NewRangeScanForPrefix(prefix)
	// 2. Set Scan Options
	opts := &gocb.ScanOptions{
		// Only retrieve the IDs (not the document content) to save bandwidth
		IDsOnly: true,
	}

	// 3. Execute the Scan
	result, err := collection.Scan(scanType, opts)
	if err != nil {
		log.Fatalf("Scan failed: %v", err)
	}

	count := 0
	// 4. Iterate over the results
	for {
		item := result.Next()
		if item == nil || item.ID() == "" {
			break
		}
		id := item.ID()
		// Process the document ID
		count++
		if count%100000 == 0 {
			log.Printf("Processed %d document IDs like %s", count, id)
		}
	}
	log.Printf("Total ids processed: %d", count)
}
