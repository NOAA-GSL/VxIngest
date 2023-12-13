package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
)

// init runs before main() is evaluated
func init() {
	log.Println("utils:init()")
}

func getTabbedString(count int) (rv string) {
	rv = ""
	for i := 0; i < count; i++ {
		rv = rv + "\t"
	}
	return rv
}

func printStringArray(in []string) {
	for i := 0; i < len(in); i++ {
		fmt.Println(in[i])
	}
}

func jsonPrettyPrint(in []interface{}) string {
	jsonText, err := json.Marshal(in)
	if err != nil {
		fmt.Println("ERROR PROCESSING STREAMING OUTPUT:", err)
	}
	var out bytes.Buffer
	json.Indent(&out, jsonText, "", "\t")
	return out.String()
}

func walkJsonMap(val map[string]interface{}, depth int) {
	for k, v := range val {
		switch vv := v.(type) {
		case string:
			fmt.Println(getTabbedString(depth), k, ":", vv, " (string)")
		case float64:
			fmt.Println(getTabbedString(depth), k, ":", vv, " (float64)")
		case []interface{}:
			fmt.Println(getTabbedString(depth), k, ":", " (array)")
			for i, u := range vv {
				fmt.Println(getTabbedString(depth+1), i, u)
			}
		case map[string]interface{}:
			fmt.Println(getTabbedString(depth), k, ":", " (map)")
			m := v.(map[string]interface{})
			walkJsonMap(m, depth+1)
		default:
			fmt.Println(getTabbedString(depth), k, vv, " (unknown)")
		}
	}
}
