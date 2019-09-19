// Testdaten erstellen
// https://stackoverflow.com/questions/16797380/how-to-create-a-10mb-file-filled-with-000000-data-in-golang

package main

import (
	"log"
	"os"
	"math/rand"
	"encoding/base64"

)

func main()  {
	createTestFiles()	
}

func createTestFiles() {

	size := int64(960 * 1024)

	token := make([]byte, size)
	token2 := make([]byte, size)
	// produce a file which has the given size in ascii encoding
	rand.Read(token)
	// produce a file which is nearly the given size in base64 encoding
	var values []byte
	
	for i:=0; i < int(size); i++{
			values = append(values, byte(rand.Intn(63)))
			basebyte := base64.StdEncoding.EncodeToString(values)
		if len(basebyte) > int(size) {
			token2 = values
			break
		}
	}

	fd, err := os.Create("output-960Kibi-rand-base64")
	if err != nil {
		log.Fatal("Failed to create output")
	}
	// _, err = fd.Seek(size-1, 0)
	// if err != nil {
	// 	log.Fatal("Failed to seek")
	// }
	_, err = fd.Write(token2)
	if err != nil {
		log.Fatal("Write failed")
	}
	err = fd.Close()
	if err != nil {
		log.Fatal("Failed to close file")
	}
}
