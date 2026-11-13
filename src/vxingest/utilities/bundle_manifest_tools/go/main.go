package main

import (
        "archive/tar"
	"bytes"
	"compress/gzip"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"
)

// orderedMap preserves insertion order of keys when marshaling to JSON,
// matching Python 3.7+ dict ordering behavior.
type orderedMap struct {
	keys   []string
	values map[string]any
}

func newOrderedMap() *orderedMap {
	return &orderedMap{
		values: make(map[string]any),
	}
}

func (om *orderedMap) set(key string, val any) {
	if _, exists := om.values[key]; !exists {
		om.keys = append(om.keys, key)
	}
	om.values[key] = val
}

func (om *orderedMap) MarshalJSON() ([]byte, error) {
	var buf bytes.Buffer
	buf.WriteByte('{')
	for i, k := range om.keys {
		if i > 0 {
			buf.WriteByte(',')
		}
		keyBytes, err := json.Marshal(k)
		if err != nil {
			return nil, err
		}
		valBytes, err := json.Marshal(om.values[k])
		if err != nil {
			return nil, err
		}
		buf.Write(keyBytes)
		buf.WriteByte(':')
		buf.Write(valBytes)
	}
	buf.WriteByte('}')
	return buf.Bytes(), nil
}

func main() {
	manifestName := flag.String("manifest-name", "MANIFEST", "Entry name for the manifest at the archive root")
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s [--manifest-name NAME] <archive.tar.gz>\n\n", os.Args[0])
		fmt.Fprintln(os.Stderr, "Generate a merged manifest from JSON files in a .tar.gz archive and insert it at the archive root.")
		flag.PrintDefaults()
	}
	flag.Parse()

	if flag.NArg() != 1 {
		flag.Usage()
		os.Exit(2)
	}

	archiveArg := expandTilde(flag.Arg(0))
	archivePath, err := filepath.Abs(archiveArg)
	if err != nil {
		fatalf("Failed to resolve path: %v", err)
	}

	if err := insertManifestIntoArchive(archivePath, *manifestName); err != nil {
		fatalf("%v", err)
	}
}

func expandTilde(p string) string {
	if p == "~" || strings.HasPrefix(p, "~/") {
		if home, err := os.UserHomeDir(); err == nil {
			return filepath.Join(home, strings.TrimPrefix(p, "~"))
		}
	}
	return p
}

func fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "Error: "+format+"\n", args...)
	os.Exit(1)
}

func cleanArchiveParts(memberName string) []string {
	raw := strings.Split(memberName, "/")
	parts := make([]string, 0, len(raw))
	for _, p := range raw {
		if p != "" && p != "." {
			parts = append(parts, p)
		}
	}
	return parts
}

func cleanArchivePath(memberName string) string {
	cleaned := path.Clean(memberName)
	cleaned = strings.TrimPrefix(cleaned, "./")
	if cleaned == "." {
		return ""
	}
	return cleaned
}

func hasExcludedDataSegment(parts []string) bool {
	for _, part := range parts {
		stem := strings.TrimSuffix(part, path.Ext(part))
		if part == "data" || stem == "data" {
			return true
		}
	}
	return false
}

func stripDataKey(rec map[string]any) map[string]any {
	out := make(map[string]any, len(rec))
	for k, v := range rec {
		if k != "data" {
			out[k] = v
		}
	}
	return out
}

func insertManifestIntoArchive(archivePath, manifestName string) error {
	info, err := os.Stat(archivePath)
	if err != nil || info.IsDir() {
		return fmt.Errorf("archive not found: %s", archivePath)
	}

	var records []map[string]any
	var allKeys []string
	seenKeys := make(map[string]bool)
	processedFiles := 0

	// Pass 1: Stream through the archive to build the manifest in memory
	f, err := os.Open(archivePath)
	if err != nil {
		return err
	}
	gzr, err := gzip.NewReader(f)
	if err != nil {
		f.Close()
		return fmt.Errorf("failed to open gzip stream: %w", err)
	}
	tr := tar.NewReader(gzr)

	for {
		hdr, err := tr.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			gzr.Close()
			f.Close()
			return fmt.Errorf("error reading tar stream: %w", err)
		}

		// Only regular files
		if hdr.Typeflag != tar.TypeReg && hdr.Typeflag != tar.TypeRegA {
			continue
		}

		parts := cleanArchiveParts(hdr.Name)
		if len(parts) == 0 {
			continue
		}

		filename := parts[len(parts)-1]

		// Exclude any path containing a "data" directory or a file named "data" / "data.json"
		if hasExcludedDataSegment(parts) {
			continue
		}

		cleanName := cleanArchivePath(hdr.Name)

		// Ignore non-.json, "LJ:" files, macOS metadata (._*), and pre-existing root manifest
		if !strings.HasSuffix(filename, ".json") ||
			strings.HasPrefix(filename, "LJ:") ||
			strings.HasPrefix(filename, "._") ||
			cleanName == manifestName ||
			cleanName == manifestName+".json" {
			continue
		}

		// Use UseNumber() so integers/floats preserve exact JSON formatting without float64 precision loss
		dec := json.NewDecoder(tr)
		dec.UseNumber()

		var parsed any
		if err := dec.Decode(&parsed); err != nil {
			gzr.Close()
			f.Close()
			return fmt.Errorf("failed to parse JSON in %s: %w", hdr.Name, err)
		}

		switch val := parsed.(type) {
		case map[string]any:
			cleanedRec := stripDataKey(val)
			for k := range val {
				if k != "data" && !seenKeys[k] {
					seenKeys[k] = true
					allKeys = append(allKeys, k)
				}
			}
			records = append(records, cleanedRec)
			processedFiles++

		case []any:
			var dictItems []map[string]any
			for _, item := range val {
				if obj, ok := item.(map[string]any); ok {
					cleanedRec := stripDataKey(obj)
					for k := range obj {
						if k != "data" && !seenKeys[k] {
							seenKeys[k] = true
							allKeys = append(allKeys, k)
						}
					}
					dictItems = append(dictItems, cleanedRec)
				}
			}
			if len(dictItems) > 0 {
				records = append(records, dictItems...)
				processedFiles++
			} else {
				fmt.Printf("Skipping non-object JSON array in: %s\n", hdr.Name)
			}

		default:
			fmt.Printf("Skipping primitive JSON value in: %s\n", hdr.Name)
		}
	}

	gzr.Close()
	f.Close()

	if len(records) == 0 {
		return fmt.Errorf("no valid JSON object records found inside %s", archivePath)
	}

	// Build manifest preserving key order
	manifest := newOrderedMap()
	for _, key := range allKeys {
		uniqueSerialized := make(map[string]bool)
		var uniqueValues []any
		presentInAll := true

		for _, rec := range records {
			val, exists := rec[key]
			if !exists {
				presentInAll = false
				continue
			}

			// Go's encoding/json automatically sorts map keys alphabetically,
			// matching Python's json.dumps(val, sort_keys=True)
			serializedBytes, err := json.Marshal(val)
			if err != nil {
				return fmt.Errorf("failed to serialize value for key %q: %w", key, err)
			}
			serialized := string(serializedBytes)

			if !uniqueSerialized[serialized] {
				uniqueSerialized[serialized] = true
				uniqueValues = append(uniqueValues, val)
			}
		}

		if len(uniqueValues) == 1 && presentInAll {
			manifest.set(key, uniqueValues[0])
		} else {
			manifest.set(key, uniqueValues)
		}
	}

	// Pretty-print with 2-space indent and disabled HTML escaping (matching Python's json.dumps)
	var manifestBuf bytes.Buffer
	enc := json.NewEncoder(&manifestBuf)
	enc.SetIndent("", "  ")
	enc.SetEscapeHTML(false)
	if err := enc.Encode(manifest); err != nil {
		return fmt.Errorf("failed to encode manifest: %w", err)
	}
	// Trim trailing newline added by json.Encoder to match json.dumps
	manifestBytes := bytes.TrimRight(manifestBuf.Bytes(), "\n")

	targetArcname := manifestName

	// Pass 2: Stream existing entries + new root 'manifest' into a temp .tar.gz, then atomically replace
	tmpFile, err := os.CreateTemp(filepath.Dir(archivePath), "*.tmp.tar.gz")
	if err != nil {
		return fmt.Errorf("failed to create temp archive: %w", err)
	}
	tmpPath := tmpFile.Name()

	cleanupTmp := func() {
		tmpFile.Close()
		os.Remove(tmpPath)
	}

	srcFile, err := os.Open(archivePath)
	if err != nil {
		cleanupTmp()
		return err
	}
	srcGz, err := gzip.NewReader(srcFile)
	if err != nil {
		srcFile.Close()
		cleanupTmp()
		return err
	}
	srcTar := tar.NewReader(srcGz)

	dstGz := gzip.NewWriter(tmpFile)
	dstTar := tar.NewWriter(dstGz)

	for {
		hdr, err := srcTar.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			srcGz.Close()
			srcFile.Close()
			cleanupTmp()
			return fmt.Errorf("error copying tar stream: %w", err)
		}

		cleanName := cleanArchivePath(hdr.Name)
		if cleanName == targetArcname {
			continue
		}

		// Copy header and stream payload directly
		hdrCopy := *hdr
		if err := dstTar.WriteHeader(&hdrCopy); err != nil {
			srcGz.Close()
			srcFile.Close()
			cleanupTmp()
			return err
		}
		if hdr.Size > 0 {
			if _, err := io.Copy(dstTar, srcTar); err != nil {
				srcGz.Close()
				srcFile.Close()
				cleanupTmp()
				return err
			}
		}
	}

	srcGz.Close()
	srcFile.Close()

	// Write new root 'manifest' entry
	newHdr := &tar.Header{
		Name:     targetArcname,
		Mode:     0644,
		Size:     int64(len(manifestBytes)),
		ModTime:  time.Now(),
		Typeflag: tar.TypeReg,
	}
	if err := dstTar.WriteHeader(newHdr); err != nil {
		cleanupTmp()
		return err
	}
	if _, err := dstTar.Write(manifestBytes); err != nil {
		cleanupTmp()
		return err
	}

	if err := dstTar.Close(); err != nil {
		cleanupTmp()
		return err
	}
	if err := dstGz.Close(); err != nil {
		cleanupTmp()
		return err
	}
	if err := tmpFile.Close(); err != nil {
		os.Remove(tmpPath)
		return err
	}

	// Preserve original file permissions and atomically overwrite archive
	if err := os.Chmod(tmpPath, info.Mode().Perm()); err != nil {
		os.Remove(tmpPath)
		return err
	}
	if err := os.Rename(tmpPath, archivePath); err != nil {
		os.Remove(tmpPath)
		return err
	}

	fmt.Printf(
		"Streamed %d files (%d records) and inserted '%s' at root of %s\n",
		processedFiles,
		len(records),
		targetArcname,
		filepath.Base(archivePath),
	)
	return nil
}
