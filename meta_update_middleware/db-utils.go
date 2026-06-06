package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/couchbase/gocb/v2"
)

type CbConnection struct {
	Cluster    *gocb.Cluster
	Bucket     *gocb.Bucket
	Scope      *gocb.Scope
	Collection *gocb.Collection
	vxDBTARGET string
}

var queryProfilingConfig = struct {
	metricsEnabled  bool
	profileMode     gocb.QueryProfileMode
	slowQueryCutoff time.Duration
}{
	metricsEnabled:  true,
	profileMode:     gocb.QueryProfileModeNone,
	slowQueryCutoff: 500 * time.Millisecond,
}

type querySummary struct {
	Tag            string
	QueryText      string
	Count          int
	TotalElapsed   time.Duration
	TotalExecution time.Duration
	MaxElapsed     time.Duration
}

var querySummaryState = struct {
	sync.Mutex
	byKey map[string]*querySummary
}{
	byKey: map[string]*querySummary{},
}

// init runs before main() is evaluated
func init() {
	log.Println("db-utils:init()")
}

func setQueryProfilingOptions(metrics bool, profile string, slowMs int) {
	queryProfilingConfig.metricsEnabled = metrics

	switch strings.ToLower(strings.TrimSpace(profile)) {
	case "", "off", "none":
		queryProfilingConfig.profileMode = gocb.QueryProfileModeNone
	case "phases":
		queryProfilingConfig.profileMode = gocb.QueryProfileModePhases
	case "timings":
		queryProfilingConfig.profileMode = gocb.QueryProfileModeTimings
	default:
		log.Fatalf("invalid -query-profile value %q, expected off|phases|timings", profile)
	}

	if slowMs < 0 {
		slowMs = 0
	}
	queryProfilingConfig.slowQueryCutoff = time.Duration(slowMs) * time.Millisecond

	log.Printf("query profiling configured: metrics=%t profile=%s slow_query_ms=%d", queryProfilingConfig.metricsEnabled, queryProfilingConfig.profileMode, slowMs)
}

func newQueryOptions() *gocb.QueryOptions {
	return &gocb.QueryOptions{
		Adhoc:   true,
		Metrics: queryProfilingConfig.metricsEnabled,
		Profile: queryProfilingConfig.profileMode,
	}
}

func finalizeQueryResult(tag string, queryText string, start time.Time, queryResult *gocb.QueryResult) {
	if err := queryResult.Err(); err != nil {
		log.Fatal(err)
	}

	meta, err := queryResult.MetaData()
	if err != nil {
		log.Fatal(err)
	}

	elapsed := time.Since(start)
	recordQuerySummary(tag, queryText, elapsed, meta.Metrics.ExecutionTime)
	if queryProfilingConfig.slowQueryCutoff > 0 && elapsed < queryProfilingConfig.slowQueryCutoff {
		return
	}

	if meta.Metrics.ElapsedTime == 0 && meta.Metrics.ExecutionTime == 0 {
		return
	}

	log.Printf("query profile [%s] elapsed=%v execution=%v count=%d warnings=%d status=%s", tag, meta.Metrics.ElapsedTime, meta.Metrics.ExecutionTime, meta.Metrics.ResultCount, len(meta.Warnings), meta.Status)
	log.Printf("query text [%s]: %s", tag, strings.TrimSpace(queryText))

	if queryProfilingConfig.profileMode != gocb.QueryProfileModeNone && meta.Profile != nil {
		profileBytes, marshalErr := json.Marshal(meta.Profile)
		if marshalErr != nil {
			log.Printf("query profile json marshal failed [%s]: %v", tag, marshalErr)
			return
		}
		log.Printf("query profile details [%s]: %s", tag, string(profileBytes))
	}
}

func recordQuerySummary(tag string, queryText string, elapsed time.Duration, execution time.Duration) {
	key := tag + "|" + strings.TrimSpace(queryText)

	querySummaryState.Lock()
	defer querySummaryState.Unlock()

	s, ok := querySummaryState.byKey[key]
	if !ok {
		s = &querySummary{Tag: tag, QueryText: strings.TrimSpace(queryText)}
		querySummaryState.byKey[key] = s
	}

	s.Count++
	s.TotalElapsed += elapsed
	s.TotalExecution += execution
	if elapsed > s.MaxElapsed {
		s.MaxElapsed = elapsed
	}
}

func printQueryProfilingSummary(limit int) {
	querySummaryState.Lock()
	if len(querySummaryState.byKey) == 0 {
		querySummaryState.Unlock()
		log.Println("query summary: no query data captured")
		return
	}

	items := make([]querySummary, 0, len(querySummaryState.byKey))
	for _, s := range querySummaryState.byKey {
		items = append(items, *s)
	}
	querySummaryState.Unlock()

	sort.Slice(items, func(i, j int) bool {
		if items[i].TotalElapsed == items[j].TotalElapsed {
			return items[i].MaxElapsed > items[j].MaxElapsed
		}
		return items[i].TotalElapsed > items[j].TotalElapsed
	})

	if limit <= 0 || limit > len(items) {
		limit = len(items)
	}

	log.Printf("query summary: %d distinct query templates (%d shown)", len(items), limit)
	for i := 0; i < limit; i++ {
		s := items[i]
		avgElapsed := time.Duration(0)
		avgExecution := time.Duration(0)
		if s.Count > 0 {
			avgElapsed = s.TotalElapsed / time.Duration(s.Count)
			avgExecution = s.TotalExecution / time.Duration(s.Count)
		}
		log.Printf("query summary #%d [%s] count=%d total_elapsed=%v avg_elapsed=%v max_elapsed=%v avg_execution=%v", i+1, s.Tag, s.Count, s.TotalElapsed, avgElapsed, s.MaxElapsed, avgExecution)
		log.Printf("query summary sql #%d: %s", i+1, s.QueryText)
	}
}

func getDbConnection(cred Credentials) (conn CbConnection) {
	log.Println("getDbConnection()")

	conn = CbConnection{}
	connectionString := cred.Cb_host
	bucketName := cred.Cb_bucket
	collection := cred.Cb_collection
	username := cred.Cb_user
	password := cred.Cb_password
	timeout := cred.Cb_timeout_seconds
	ca_cert_str := ""
	if strings.Contains(connectionString, "cloud.couchbase.com") {
		ca_cert_str = "--cacert " + os.Getenv("CACERT_FILE")
	}
	connectionString = connectionString + " " + ca_cert_str
	options := gocb.ClusterOptions{
		Authenticator: gocb.PasswordAuthenticator{
			Username: username,
			Password: password,
		},
		TimeoutsConfig: gocb.TimeoutsConfig{
			QueryTimeout: time.Duration(timeout) * time.Second,
		},
	}

	cluster, err := gocb.Connect(connectionString, options)
	if err != nil {
		log.Fatal(err)
		return
	}

	conn.Cluster = cluster
	conn.Bucket = conn.Cluster.Bucket(bucketName)
	conn.Collection = conn.Bucket.Collection(collection)
	conn.vxDBTARGET = cred.Cb_bucket + "." + cred.Cb_scope + "." + cred.Cb_collection

	log.Println("vxDBTARGET:" + conn.vxDBTARGET)

	err = conn.Bucket.WaitUntilReady(5*time.Second, nil)
	if err != nil {
		log.Fatal(err)
		return
	}

	conn.Scope = conn.Bucket.Scope(cred.Cb_scope)
	return conn
}

func queryWithSQLFile(scope *gocb.Scope, file string) (jsonOut []string) {
	fileContent, err := os.ReadFile(file)
	if err != nil {
		log.Fatal(err)
	}

	// Convert []byte to string
	text := string(fileContent)
	fmt.Println(text)

	return queryWithSQLStringSA(scope, text)
}

func queryWithSQLStringSA(scope *gocb.Scope, text string) (rv []string) {
	log.Println("queryWithSQLStringSA(\n" + text + "\n)")
	start := time.Now()

	queryResult, err := scope.Query(
		text,
		newQueryOptions(),
	)
	if err != nil {
		log.Fatal(err)
	}

	// Interfaces for handling streaming return values
	retValues := []string{}

	// Stream the values returned from the query into an untyped and unstructred
	// array of interfaces
	for queryResult.Next() {
		var row interface{}
		err := queryResult.Row(&row)
		if err != nil {
			log.Fatal(err)
		}
		retValues = append(retValues, row.(string))
	}

	finalizeQueryResult("queryWithSQLStringSA", text, start, queryResult)

	return retValues
}

func queryWithSQLStringFA(scope *gocb.Scope, text string) (rv []float64) {
	log.Println("queryWithSQLStringFA(\n" + text + "\n)")
	start := time.Now()

	queryResult, err := scope.Query(
		text,
		newQueryOptions(),
	)
	if err != nil {
		log.Fatal(err)
	}

	retValues := make([]float64, 0)

	// Stream the values returned from the query into an untyped and unstructred
	// array of interfaces
	for queryResult.Next() {
		var row interface{}
		err := queryResult.Row(&row)
		if err != nil {
			log.Fatal(err)
		}
		retValues = append(retValues, row.(float64))
	}

	finalizeQueryResult("queryWithSQLStringFA", text, start, queryResult)

	return retValues
}

func queryWithSQLStringIA(scope *gocb.Scope, text string) (rv []int) {
	log.Println("queryWithSQLStringIA(\n" + text + "\n)")
	start := time.Now()

	queryResult, err := scope.Query(
		text,
		newQueryOptions(),
	)
	if err != nil {
		log.Fatal(err)
	}

	retValues := make([]int, 0)

	// Stream the values returned from the query into an untyped and unstructred
	// array of interfaces
	for queryResult.Next() {
		var row interface{}
		err := queryResult.Row(&row)
		if err != nil {
			log.Fatal(err)
		}
		switch v := row.(type) {
		case float64:
			retValues = append(retValues, int(v))
		case int:
			retValues = append(retValues, v)
		}
	}

	finalizeQueryResult("queryWithSQLStringIA", text, start, queryResult)

	return retValues
}

func queryWithSQLStringMAP(scope *gocb.Scope, text string) (jsonOut []interface{}) {
	log.Println("queryWithSQLStringMAP(\n" + text + "\n)")
	start := time.Now()

	queryResult, err := scope.Query(
		text,
		newQueryOptions(),
	)
	if err != nil {
		log.Fatal(err)
	}

	rows := make([]interface{}, 0)

	for queryResult.Next() {
		var row interface{}
		err := queryResult.Row(&row)
		if err != nil {
			log.Fatal(err)
		}
		if m, ok := row.(map[string]interface{}); ok {
			rows = append(rows, m)
		} else if s, ok := row.(string); ok {
			rows = append(rows, s)
		}
	}

	finalizeQueryResult("queryWithSQLStringMAP", text, start, queryResult)
	return rows
}

func queryWithSQLFileJustPrint(scope *gocb.Scope, file string) {
	fileContent, err := os.ReadFile(file)
	if err != nil {
		log.Fatal(err)
	}

	// Convert []byte to string
	text := string(fileContent)
	fmt.Println(text)

	queryResult, err := scope.Query(
		text,
		newQueryOptions(),
	)
	if err != nil {
		log.Fatal(err)
	} else {
		printQueryResult(queryResult)
	}
}

func printQueryResult(queryResult *gocb.QueryResult) {
	for queryResult.Next() {
		var result interface{}
		err := queryResult.Row(&result)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println(result)
	}
}
