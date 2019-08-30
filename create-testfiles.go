// Testdaten erstellen
// https://stackoverflow.com/questions/16797380/how-to-create-a-10mb-file-filled-with-000000-data-in-golang

package main

import (
	"log"
	"os"
)

func main() {
	size := int64(1 * 1024 * 1024)
	fd, err := os.Create("output-1Mibi")
	if err != nil {
		log.Fatal("Failed to create output")
	}
	_, err = fd.Seek(size-1, 0)
	if err != nil {
		log.Fatal("Failed to seek")
	}
	_, err = fd.Write([]byte{0})
	if err != nil {
		log.Fatal("Write failed")
	}
	err = fd.Close()
	if err != nil {
		log.Fatal("Failed to close file")
	}
}
