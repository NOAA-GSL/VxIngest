package main

import (
	"bytes"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"log"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/couchbase/gocb/v2"
)

func writeTestCACertPEM(t *testing.T, dir string) string {
	t.Helper()

	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("failed to generate rsa key: %v", err)
	}

	tpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "unit-test-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}

	der, err := x509.CreateCertificate(rand.Reader, tpl, tpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("failed to create cert: %v", err)
	}

	pemData := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	path := filepath.Join(dir, "ca.pem")
	if err := os.WriteFile(path, pemData, 0o600); err != nil {
		t.Fatalf("failed to write cert file: %v", err)
	}

	return path
}

func resetQuerySummaryState() {
	querySummaryState.Lock()
	defer querySummaryState.Unlock()
	querySummaryState.byKey = map[string]*querySummary{}
}

func TestSetQueryProfilingOptions_SetsModesAndCutoff(t *testing.T) {
	setQueryProfilingOptions(true, "off", 250)
	if !queryProfilingConfig.metricsEnabled {
		t.Fatalf("expected metrics to be enabled")
	}
	if queryProfilingConfig.profileMode != gocb.QueryProfileModeNone {
		t.Fatalf("expected profile mode off, got %v", queryProfilingConfig.profileMode)
	}
	if queryProfilingConfig.slowQueryCutoff != 250*time.Millisecond {
		t.Fatalf("expected cutoff 250ms, got %v", queryProfilingConfig.slowQueryCutoff)
	}

	setQueryProfilingOptions(false, "phases", 10)
	if queryProfilingConfig.metricsEnabled {
		t.Fatalf("expected metrics to be disabled")
	}
	if queryProfilingConfig.profileMode != gocb.QueryProfileModePhases {
		t.Fatalf("expected profile mode phases, got %v", queryProfilingConfig.profileMode)
	}

	setQueryProfilingOptions(true, "timings", 15)
	if queryProfilingConfig.profileMode != gocb.QueryProfileModeTimings {
		t.Fatalf("expected profile mode timings, got %v", queryProfilingConfig.profileMode)
	}
}

func TestSetQueryProfilingOptions_NegativeSlowMsClampedToZero(t *testing.T) {
	setQueryProfilingOptions(true, "off", -1)
	if queryProfilingConfig.slowQueryCutoff != 0 {
		t.Fatalf("expected slow query cutoff to be 0, got %v", queryProfilingConfig.slowQueryCutoff)
	}
}

func TestNewQueryOptions_ReflectsCurrentProfilingConfig(t *testing.T) {
	setQueryProfilingOptions(false, "phases", 123)
	opts := newQueryOptions()

	if opts.Adhoc != true {
		t.Fatalf("expected Adhoc=true")
	}
	if opts.Metrics != false {
		t.Fatalf("expected Metrics=false, got %v", opts.Metrics)
	}
	if opts.Profile != gocb.QueryProfileModePhases {
		t.Fatalf("expected Profile=phases, got %v", opts.Profile)
	}
}

func TestRecordQuerySummary_AggregatesByTrimmedQueryText(t *testing.T) {
	resetQuerySummaryState()

	recordQuerySummary("q", " SELECT 1 ", 120*time.Millisecond, 80*time.Millisecond)
	recordQuerySummary("q", "SELECT 1", 30*time.Millisecond, 10*time.Millisecond)
	recordQuerySummary("q", "SELECT 2", 50*time.Millisecond, 25*time.Millisecond)

	querySummaryState.Lock()
	defer querySummaryState.Unlock()

	if len(querySummaryState.byKey) != 2 {
		t.Fatalf("expected 2 summary keys, got %d", len(querySummaryState.byKey))
	}

	s := querySummaryState.byKey["q|SELECT 1"]
	if s == nil {
		t.Fatalf("expected summary for key q|SELECT 1")
	}
	if s.Count != 2 {
		t.Fatalf("expected count=2, got %d", s.Count)
	}
	if s.TotalElapsed != 150*time.Millisecond {
		t.Fatalf("expected total elapsed 150ms, got %v", s.TotalElapsed)
	}
	if s.TotalExecution != 90*time.Millisecond {
		t.Fatalf("expected total execution 90ms, got %v", s.TotalExecution)
	}
	if s.MaxElapsed != 120*time.Millisecond {
		t.Fatalf("expected max elapsed 120ms, got %v", s.MaxElapsed)
	}
}

func TestPrintQueryProfilingSummary_NoData(t *testing.T) {
	resetQuerySummaryState()

	var buf bytes.Buffer
	oldWriter := log.Writer()
	oldFlags := log.Flags()
	log.SetOutput(&buf)
	log.SetFlags(0)
	defer func() {
		log.SetOutput(oldWriter)
		log.SetFlags(oldFlags)
	}()

	printQueryProfilingSummary(10)

	out := buf.String()
	if !strings.Contains(out, "query summary: no query data captured") {
		t.Fatalf("expected no-data summary log, got: %s", out)
	}
}

func TestPrintQueryProfilingSummary_HonorsLimit(t *testing.T) {
	resetQuerySummaryState()
	recordQuerySummary("A", "SELECT 1", 300*time.Millisecond, 200*time.Millisecond)
	recordQuerySummary("B", "SELECT 2", 100*time.Millisecond, 90*time.Millisecond)
	recordQuerySummary("C", "SELECT 3", 50*time.Millisecond, 40*time.Millisecond)

	var buf bytes.Buffer
	oldWriter := log.Writer()
	oldFlags := log.Flags()
	log.SetOutput(&buf)
	log.SetFlags(0)
	defer func() {
		log.SetOutput(oldWriter)
		log.SetFlags(oldFlags)
	}()

	printQueryProfilingSummary(2)

	out := buf.String()
	if !strings.Contains(out, "query summary: 3 distinct query templates (2 shown)") {
		t.Fatalf("unexpected summary header: %s", out)
	}
	if strings.Contains(out, "query summary sql #3") {
		t.Fatalf("expected only 2 rows in summary, got output: %s", out)
	}
}

func TestConfigureCapellaTLSOptions_NonCloud_NoOp(t *testing.T) {
	options := gocb.ClusterOptions{}
	err := configureCapellaTLSOptions("couchbase://localhost", &options)
	if err != nil {
		t.Fatalf("expected no error for non-cloud host, got: %v", err)
	}
	if options.SecurityConfig.TLSRootCAs != nil {
		t.Fatalf("expected TLSRootCAs to remain nil for non-cloud host")
	}
}

func TestConfigureCapellaTLSOptions_Cloud_RequiresCACertPath(t *testing.T) {
	options := gocb.ClusterOptions{}
	err := configureCapellaTLSOptions("couchbases://foo.cloud.couchbase.com", &options)
	if err == nil {
		t.Fatalf("expected error when CACERT_FILE is missing")
	}
	if !strings.Contains(err.Error(), "CACERT_FILE must be set") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestConfigureCapellaTLSOptions_Cloud_SetsTLSRootCAs(t *testing.T) {
	dir := t.TempDir()
	writeTestCACertPEM(t, dir)
	os.Setenv("CACERT_FILE", dir+"/cacert.pem")
	os.Setenv("CACERT_REQUIRED","true")
	defer os.Unsetenv("CACERT_FILE")
	defer os.Unsetenv("CACERT_REQUIRED")

	options := gocb.ClusterOptions{}
	err := configureCapellaTLSOptions("couchbases://foo.cloud.couchbase.com", &options)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}
	if options.SecurityConfig.TLSRootCAs == nil {
		t.Fatalf("expected TLSRootCAs to be configured")
	}
}
