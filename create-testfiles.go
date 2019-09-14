// Testdaten erstellen
// https://stackoverflow.com/questions/16797380/how-to-create-a-10mb-file-filled-with-000000-data-in-golang

package main

import (
	"log"
	"os"
	"math/rand"
)

func createTestFiles() {

	size := int64(100 * 1024)

    token := make([]byte, size)
    rand.Read(token)

	fd, err := os.Create("output-100Kibi-rand")
	if err != nil {
		log.Fatal("Failed to create output")
	}
	// _, err = fd.Seek(size-1, 0)
	// if err != nil {
	// 	log.Fatal("Failed to seek")
	// }
	_, err = fd.Write(token)
	if err != nil {
		log.Fatal("Write failed")
	}
	err = fd.Close()
	if err != nil {
		log.Fatal("Failed to close file")
	}
}
