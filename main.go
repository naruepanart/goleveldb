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

	// Test 1: Write then delete
	fmt.Println("Testing delete operation...")
	err = db.Put(nil, []byte("to_delete"), []byte("will_be_deleted"))
	if err != nil {
		log.Fatal("Put failed:", err)
	}

	// Verify the value exists
	value, err := db.Get(nil, []byte("to_delete"))
	if err != nil {
		log.Fatal("Get before delete failed:", err)
	}
	fmt.Printf("Before delete: %s\n", string(value))

	// Delete the key
	err = db.Delete(nil, []byte("to_delete"))
	if err != nil {
		log.Fatal("Delete failed:", err)
	}
	fmt.Println("Delete operation completed")

	// Verify the value is gone
	_, err = db.Get(nil, []byte("to_delete"))
	if err != nil {
		fmt.Println("✅ Key correctly deleted (not found)")
	} else {
		fmt.Println("❌ Key still exists after deletion")
	}

	// Test 2: Delete non-existent key (should not error)
	err = db.Delete(nil, []byte("non_existent_key"))
	if err != nil {
		fmt.Printf("Delete non-existent key failed: %v\n", err)
	} else {
		fmt.Println("✅ Delete non-existent key completed (no error)")
	}

	fmt.Println("Delete test completed")
}
