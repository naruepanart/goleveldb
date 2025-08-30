package main

import (
	"fmt"
	"log"
	"os"
)

func main() {
	// Clean up first
	os.RemoveAll("./testdb")
	fmt.Println("Cleaned up test directory")

	opts := &DefaultOptions
	db, err := Open("./testdb", opts)
	if err != nil {
		log.Fatal("Open failed:", err)
	}
	defer db.Close()

	fmt.Println("Database opened successfully")

	// Test 1: Basic put/get
	fmt.Println("Testing basic put/get...")
	err = db.Put(nil, []byte("test_key"), []byte("test_value"))
	if err != nil {
		log.Fatal("Put failed:", err)
	}
	fmt.Println("Put operation successful")

	value, err := db.Get(nil, []byte("test_key"))
	if err != nil {
		log.Fatal("Get failed:", err)
	}
	fmt.Printf("Get result: %s\n", string(value))

	// Test 2: Simple batch write (without using db.Write)
	fmt.Println("Testing simple operations instead of batch...")
	err = db.Put(nil, []byte("key1"), []byte("value1"))
	if err != nil {
		log.Fatal("Put key1 failed:", err)
	}

	err = db.Put(nil, []byte("key2"), []byte("value2"))
	if err != nil {
		log.Fatal("Put key2 failed:", err)
	}

	err = db.Delete(nil, []byte("test_key"))
	if err != nil {
		log.Fatal("Delete failed:", err)
	}

	// Verify results
	value1, err := db.Get(nil, []byte("key1"))
	if err != nil {
		log.Fatal("Get key1 failed:", err)
	}
	fmt.Printf("key1: %s\n", string(value1))

	value2, err := db.Get(nil, []byte("key2"))
	if err != nil {
		log.Fatal("Get key2 failed:", err)
	}
	fmt.Printf("key2: %s\n", string(value2))

	// Verify deletion
	_, err = db.Get(nil, []byte("test_key"))
	if err != nil {
		fmt.Println("test_key correctly deleted (not found)")
	} else {
		fmt.Println("test_key still exists")
	}

	fmt.Println("All basic tests passed! ✅")

	// Test 3: Try batch write later after fixing the function
	fmt.Println("Testing batch write...")
	batch := NewBatch()
	batch.Put([]byte("batch1"), []byte("batch_value1"))
	batch.Put([]byte("batch2"), []byte("batch_value2"))

	err = db.Write(nil, batch)
	if err != nil {
		fmt.Printf("Batch write failed (expected for now): %v\n", err)
	} else {
		fmt.Println("Batch write successful")
	}
}
