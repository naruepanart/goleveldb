package main

import (
	"fmt"
	"log"
)

func main() {
	// Open the database
	opts := &Options{
		CreateIfMissing: true,
		ErrorIfExists:   false,
	}
	db, err := Open("./testdb", opts)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Write some data
	writeOpts := &WriteOptions{}
	err = db.Put(writeOpts, []byte("key1"), []byte("value1"))
	if err != nil {
		log.Fatal(err)
	}

	// Read data
	readOpts := &ReadOptions{}
	value, err := db.Get(readOpts, []byte("key1"))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("key1: %s\n", string(value))

	// Delete data
	err = db.Delete(writeOpts, []byte("key1"))
	if err != nil {
		log.Fatal(err)
	}

	// Verify deletion
	value, err = db.Get(readOpts, []byte("key1"))
	if err != nil {
		fmt.Println("key1 not found (as expected)")
	} else {
		fmt.Printf("Unexpected value found: %s\n", string(value))
	}
}
