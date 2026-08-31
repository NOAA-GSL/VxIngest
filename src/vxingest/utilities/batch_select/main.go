package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	"gopkg.in/yaml.v2"

	gocb "github.com/couchbase/gocb/v2"
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

// askUserToAppend prompts the user if they want to append to an existing file.
func askUserToAppend(filename string) bool {
	reader := bufio.NewReader(os.Stdin)
	fmt.Printf("File '%s' already exists. Append to it? (y/n): ", filename)
	response, _ := reader.ReadString('\n')
	return strings.TrimSpace(strings.ToLower(response)) == "y"
}

func paginateAllDocs(cluster *gocb.Cluster, bucketName, filename, query string, pageSize int, assumeYes bool) {
	offset := 0
	baseQuery := strings.TrimSuffix(strings.TrimSpace(query), ";")

	// Check if file exists and ask user if they want to append
	if _, err := os.Stat(filename); err == nil {
		if !assumeYes && !askUserToAppend(filename) {
			fmt.Println("Operation cancelled. File not modified.")
			return
		}
		fmt.Printf("Appending results to existing file '%s'.\n", filename)
	} else if os.IsNotExist(err) {
		fmt.Printf("Creating output file '%s'.\n", filename)
	} else {
		log.Fatalf("failed to check output file: %v", err)
	}

	// Open file for appending if it exists, or create it if it doesn't.
	file, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		log.Fatalf("failed to open file: %v", err)
	}
	defer file.Close()

	writer := bufio.NewWriter(file)
	defer writer.Flush()

	for {
		pagedQuery := fmt.Sprintf("%s LIMIT %d OFFSET %d", baseQuery, pageSize, offset)

		rows, err := cluster.Query(pagedQuery, &gocb.QueryOptions{Adhoc: true})
		if err != nil {
			log.Fatalf("query error: %v", err)
		}

		count := 0
		for rows.Next() {
			var doc map[string]interface{}
			if err := rows.Row(&doc); err != nil {
				log.Fatalf("row error: %v", err)
			}
			count++

			// Write each doc to file as JSONL immediately
			jsonBytes, err := json.Marshal(doc)
			if err != nil {
				log.Fatalf("json marshal error: %v", err)
			}
			if _, err := writer.WriteString(string(jsonBytes) + "\n"); err != nil {
				log.Fatalf("write error: %v", err)
			}
		}
		if err := rows.Err(); err != nil {
			log.Fatalf("iteration error: %v", err)
		}

		// Stop when we get fewer rows than the page size
		if count < pageSize {
			break
		}

		// Process the batch (e.g., export, transform, etc.)
		fmt.Printf("Fetched %d docs (offset %d)\n", count, offset)
		offset += pageSize
	}

	fmt.Println("Done.")
}

func main() {
	var filename string
	var query string
	var pageSize int
	var assumeYes bool
	flag.StringVar(&filename, "f", "docs.json", "output file path (default: docs.json)")
	flag.StringVar(&query, "q", "", "SQL++ query to run; LIMIT/OFFSET are added automatically")
	flag.IntVar(&pageSize, "p", 1000, "number of documents to fetch per page")
	flag.BoolVar(&assumeYes, "y", false, "append to an existing output file without prompting")
	flag.Usage = func() {
		fmt.Fprintf(flag.CommandLine.Output(), "Usage: %s -q <query> [-f output-file] [-p page-size] [-y]\n\n", os.Args[0])
		fmt.Fprintln(flag.CommandLine.Output(), "Required:")
		fmt.Fprintln(flag.CommandLine.Output(), "  -q string    SQL++ query to run. Do not include LIMIT or OFFSET; they are added automatically.")
		fmt.Fprintln(flag.CommandLine.Output(), "")
		fmt.Fprintln(flag.CommandLine.Output(), "Optional:")
		fmt.Fprintf(flag.CommandLine.Output(), "  -f string\n    \toutput file path (default %q)\n", "docs.json")
		fmt.Fprintf(flag.CommandLine.Output(), "  -p int\n    \tnumber of documents to fetch per page (default %d)\n", 1000)
		fmt.Fprintln(flag.CommandLine.Output(), "  -y")
		fmt.Fprintln(flag.CommandLine.Output(), "    \tappend to an existing output file without prompting")
		fmt.Fprintln(flag.CommandLine.Output(), "")
		fmt.Fprintln(flag.CommandLine.Output(), "Environment:")
		fmt.Fprintln(flag.CommandLine.Output(), "  CREDENTIALS_FILE must point to the Couchbase credentials YAML file.")
		fmt.Fprintln(flag.CommandLine.Output(), "")
		fmt.Fprintf(flag.CommandLine.Output(), "Example:\n  %s -q 'SELECT * FROM vxdata._default.METAR WHERE type = \"DD\"' -f docs.json -p 1000 -y\n", os.Args[0])
	}
	flag.Parse()
	if strings.TrimSpace(query) == "" {
		flag.Usage()
		os.Exit(2)
	}
	if pageSize <= 0 {
		fmt.Fprintln(flag.CommandLine.Output(), "page size must be greater than 0")
		flag.Usage()
		os.Exit(2)
	}

	credentials, err := GetCBCredentials()
	if err != nil {
		log.Fatalf("Error getting Couchbase credentials: %v", err)
	}

	cluster, err := GetConnection(credentials)
	if err != nil {
		log.Fatalf("Error connecting to Couchbase: %v", err)
	}
	defer cluster.Close(nil)

	paginateAllDocs(cluster, credentials.CBBucket, filename, query, pageSize, assumeYes)
}
