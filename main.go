package main

import (
	"fmt"
	"log"
	"os"
	"strconv"

	"./kafka"
	"./rabbit"
)

func main() {

	topictemp := "test"
	messages := 1
	countprodcon := 0
	compressionType := "json"
	messageSize := ""

	usedSystem := "kafka"

	// fill commandline parameters in programm variables
	for i := 0; i < len(os.Args); i++ {
		fmt.Printf("%d : %s \n", i, os.Args[i])

		if i == 1 {
			usedSystem = os.Args[i]
		}
		if i == 2 {
			messagesCMD, err := strconv.ParseInt(os.Args[i], 10, 64)
			if err != nil {
				log.Fatal("%s", err)
			}
			messages = int(messagesCMD)
		}
		if i == 3 {
			topictemp = os.Args[i]
		}
		if i == 4 {
			countprodconCMD, err := strconv.ParseInt(os.Args[i], 10, 64)
			if err != nil {
				log.Fatal("%s", err)
			}
			countprodcon = int(countprodconCMD)
		}
		if i == 5 {
			if os.Args[i] != "" {
				messageSize = os.Args[i]
			} else {
				fmt.Println("Missing file for binary message (generated file with fixed size of random data)")
				fmt.Println("Sending only the Time and and Identifier for every message !!!")
			}
		}
		if i == 6 {
			compressionType = os.Args[i]
		}
	}

	if usedSystem == "kafka"{
		kafka.Kafka(messages, topictemp, countprodcon, compressionType, messageSize)
	}
	if usedSystem == "rabbit"{
		rabbit.Rabbit(messages, topictemp, countprodcon, compressionType, messageSize)
	}

}
