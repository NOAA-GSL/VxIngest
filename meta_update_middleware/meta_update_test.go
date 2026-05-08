package main

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

// ---------- parseConfig ----------

func TestParseConfig_ValidSettings(t *testing.T) {
	content := `{
		"metadata": [
			{
				"name": "ceiling",
				"app": "cb-ceiling",
				"docType": ["CTC"],
				"subDocType": "CEILING"
			},
			{
				"name": "surface",
				"app": "cb-surface",
				"docType": ["SUMS"],
				"subDocType": "SURFACE"
			}
		]
	}`

	f := writeTempFile(t, "settings_*.json", content)
	defer os.Remove(f)

	conf, err := parseConfig(f)
	if err != nil {
		t.Fatalf("parseConfig returned unexpected error: %v", err)
	}
	if len(conf.Metadata) != 2 {
		t.Fatalf("expected 2 metadata entries, got %d", len(conf.Metadata))
	}

	entry := conf.Metadata[0]
	if entry.Name != "ceiling" {
		t.Errorf("expected name 'ceiling', got %q", entry.Name)
	}
	if entry.App != "cb-ceiling" {
		t.Errorf("expected app 'cb-ceiling', got %q", entry.App)
	}
	if len(entry.DocType) != 1 || entry.DocType[0] != "CTC" {
		t.Errorf("expected docType [CTC], got %v", entry.DocType)
	}
	if entry.SubDocType != "CEILING" {
		t.Errorf("expected subDocType 'CEILING', got %q", entry.SubDocType)
	}
}

func TestParseConfig_MultipleDocTypes(t *testing.T) {
	content := `{
		"metadata": [
			{
				"name": "multi",
				"app": "cb-multi",
				"docType": ["CTC", "SUMS"],
				"subDocType": "MIXED"
			}
		]
	}`

	f := writeTempFile(t, "settings_*.json", content)
	defer os.Remove(f)

	conf, err := parseConfig(f)
	if err != nil {
		t.Fatalf("parseConfig returned error: %v", err)
	}
	if len(conf.Metadata[0].DocType) != 2 {
		t.Errorf("expected 2 docTypes, got %d", len(conf.Metadata[0].DocType))
	}
}

func TestParseConfig_EmptyMetadata(t *testing.T) {
	content := `{"metadata": []}`

	f := writeTempFile(t, "settings_*.json", content)
	defer os.Remove(f)

	conf, err := parseConfig(f)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(conf.Metadata) != 0 {
		t.Errorf("expected 0 metadata entries, got %d", len(conf.Metadata))
	}
}

func TestParseConfig_MissingFile(t *testing.T) {
	_, err := parseConfig("/nonexistent/path/settings.json")
	if err == nil {
		t.Error("expected error for missing file, got nil")
	}
}

func TestParseConfig_MalformedJSON_Exits(t *testing.T) {
	if os.Getenv("TEST_PARSE_CONFIG_INVALID_JSON") == "1" {
		_, _ = parseConfig(os.Getenv("TEST_PARSE_CONFIG_INVALID_JSON_FILE"))
		return
	}

	content := `{
		"metadata": [
			{"name": "broken", "app": "cb-broken", "docType": ["CTC"], "subDocType": "CEILING"}
		`

	f := writeTempFile(t, "settings_invalid_*.json", content)
	defer os.Remove(f)

	cmd := exec.Command(os.Args[0], "-test.run=TestParseConfig_MalformedJSON_Exits")
	cmd.Env = append(
		os.Environ(),
		"TEST_PARSE_CONFIG_INVALID_JSON=1",
		"TEST_PARSE_CONFIG_INVALID_JSON_FILE="+f,
	)

	err := cmd.Run()
	if err == nil {
		t.Fatal("expected subprocess failure for malformed JSON, got nil error")
	}

	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) {
		t.Fatalf("expected *exec.ExitError, got %T (%v)", err, err)
	}
	if exitErr.ExitCode() == 0 {
		t.Fatalf("expected non-zero exit code for malformed JSON")
	}
}

// ---------- getCredentials ----------

func TestGetCredentials_AllFields(t *testing.T) {
	content := `cb_host: "couchbase://localhost"
cb_user: "testuser"
cb_password: "s3cret"
cb_bucket: "vxdata"
cb_scope: "_default"
cb_collection: "METAR"
cb_timeout_seconds: 120
`
	f := writeTempFile(t, "credentials_*", content)
	defer os.Remove(f)

	creds := getCredentials(f)

	if creds.Cb_host != "couchbase://localhost" {
		t.Errorf("expected cb_host 'couchbase://localhost', got %q", creds.Cb_host)
	}
	if creds.Cb_user != "testuser" {
		t.Errorf("expected cb_user 'testuser', got %q", creds.Cb_user)
	}
	if creds.Cb_password != "s3cret" {
		t.Errorf("expected cb_password 's3cret', got %q", creds.Cb_password)
	}
	if creds.Cb_bucket != "vxdata" {
		t.Errorf("expected cb_bucket 'vxdata', got %q", creds.Cb_bucket)
	}
	if creds.Cb_scope != "_default" {
		t.Errorf("expected cb_scope '_default', got %q", creds.Cb_scope)
	}
	if creds.Cb_collection != "METAR" {
		t.Errorf("expected cb_collection 'METAR', got %q", creds.Cb_collection)
	}
	if creds.Cb_timeout_seconds != 120 {
		t.Errorf("expected cb_timeout_seconds 120, got %d", creds.Cb_timeout_seconds)
	}
}

func TestGetCredentials_MissingOptionalTimeout(t *testing.T) {
	// cb_timeout_seconds not specified; should default to zero value (0)
	content := `cb_host: "couchbase://localhost"
cb_user: "admin"
cb_password: "pass"
cb_bucket: "vxdata"
cb_scope: "_default"
cb_collection: "METAR"
`
	f := writeTempFile(t, "credentials_*", content)
	defer os.Remove(f)

	creds := getCredentials(f)
	if creds.Cb_timeout_seconds != 0 {
		t.Errorf("expected cb_timeout_seconds 0 when not set, got %d", creds.Cb_timeout_seconds)
	}
}

// ---------- helpers ----------

// writeTempFile writes content to a temp file and returns its path.
func writeTempFile(t *testing.T, pattern, content string) string {
	t.Helper()
	f, err := os.CreateTemp(filepath.Dir(os.TempDir()), pattern)
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	if _, err := f.WriteString(content); err != nil {
		f.Close()
		os.Remove(f.Name())
		t.Fatalf("failed to write temp file: %v", err)
	}
	f.Close()
	return f.Name()
}
