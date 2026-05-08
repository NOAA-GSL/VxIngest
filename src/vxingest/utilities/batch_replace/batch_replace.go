package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
)

func main() {
	inputPath := flag.String("input", "", "Path to the input JSON file")
	mapPath := flag.String("map", "", "Path to the map JSON file")
	outputPath := flag.String("output", "", "Path to the output JSON file")
	flag.Parse()

	// 1. Validate arguments
	if *inputPath == "" || *mapPath == "" || *outputPath == "" {
		fmt.Println("Usage: go run batch_replace.go -input <input_json> -map <map_json> -output <output_json>")
		os.Exit(1)
	}

	// 2. Load and parse the name-map
	mapFile, err := os.ReadFile(*mapPath)
	if err != nil {
		fmt.Printf("Error reading map file: %v\n", err)
		os.Exit(1)
	}

	var nameMap map[string]string
	if err := json.Unmarshal(mapFile, &nameMap); err != nil {
		fmt.Printf("Error parsing map JSON: %v\n", err)
		os.Exit(1)
	}

	// 3. Prepare the Replacer
	pairs := make([]string, 0, len(nameMap)*2)
	for k, v := range nameMap {
		pairs = append(pairs, k, v)
	}
	replacer := strings.NewReplacer(pairs...)

	// 4. Open input and create output files
	inputFile, err := os.Open(*inputPath)
	if err != nil {
		fmt.Printf("Error opening input: %v\n", err)
		os.Exit(1)
	}
	defer inputFile.Close()

	outputFile, err := os.Create(*outputPath)
	if err != nil {
		fmt.Printf("Error creating output: %v\n", err)
		os.Exit(1)
	}
	defer outputFile.Close()

	// 5. Stream the replacement
	// strings.Replacer.WriteString writes the replaced version of 
	// the input stream directly to the output file.
	_, err = replacer.WriteString(outputFile, readAllString(inputFile))
	if err != nil {
		fmt.Printf("Error during replacement: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("Success! File processed and saved to:", *outputPath)
}

// Helper to read the file into a string for the replacer
func readAllString(r io.Reader) string {
	b, _ := io.ReadAll(r)
	return string(b)
}