package main

import (
	"fmt"
	"log"
	"os"
)

func main() {
	os.RemoveAll("./testdb")

	fmt.Println("=== Testing Log Recovery ===")

	// Phase 1: Write data
	db1, err := Open("./testdb", &DefaultOptions)
	if err != nil {
		log.Fatal(err)
	}

	// Write test data
	testData := map[string]string{
		"test":  "value",
		"hello": "world",
		"key1":  "value1",
	}

	for key, value := range testData {
		err := db1.Put(nil, []byte(key), []byte(value))
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("Wrote: %s = %s\n", key, value)
	}

	db1.Close()

	// Phase 2: Recovery
	fmt.Println("\n=== Attempting Recovery ===")
	db2, err := Open("./testdb", &DefaultOptions)
	if err != nil {
		log.Fatal(err)
	}

	// Test recovery
	recovered := 0
	for key, expectedValue := range testData {
		value, err := db2.Get(nil, []byte(key))
		if err == nil && string(value) == expectedValue {
			fmt.Printf("✅ %s = %s\n", key, string(value))
			recovered++
		} else {
			fmt.Printf("❌ %s = NOT FOUND (error: %v)\n", key, err)
		}
	}

	fmt.Printf("Recovery success: %d/%d\n", recovered, len(testData))

	db2.Close()
}
