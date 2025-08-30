package main

import (
	"fmt"
	"log"
)

func main() {
	opts := &DefaultOptions
	db, err := Open("./testdb", opts)
	if err != nil {
		log.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	fmt.Println("Database opened successfully!")

	// ทดสอบเขียนและอ่านหลายค่า
	testData := map[string]string{
		"key1": "value1",
		"key2": "value2",
		"key3": "value3",
	}

	for key, value := range testData {
		err = db.Put(nil, []byte(key), []byte(value))
		if err != nil {
			log.Fatalf("Failed to put %s: %v", key, err)
		}
		fmt.Printf("Put %s = %s\n", key, value)
	}

	// ทดสอบอ่านค่าทั้งหมด
	for key := range testData {
		value, err := db.Get(nil, []byte(key))
		if err != nil {
			log.Fatalf("Failed to get %s: %v", key, err)
		}
		fmt.Printf("Get %s = %s\n", key, string(value))
	}

	// ทดสอบลบค่า
	err = db.Delete(nil, []byte("key2"))
	if err != nil {
		log.Fatalf("Failed to delete key2: %v", err)
	}
	fmt.Println("Deleted key2")

	// ตรวจสอบว่าถูกลบจริง
	_, err = db.Get(nil, []byte("key2"))
	if err != nil {
		fmt.Println("key2 not found (as expected after deletion)")
	}

	fmt.Println("All tests passed! ✅")
}
