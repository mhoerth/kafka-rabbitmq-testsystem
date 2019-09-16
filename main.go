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

	// set default values to avoid programm blocking if no parameter is set by the user
	topictemp := "test"
	messages := 1
	countprodcon := 0
	compressionType := "json"
	messageSize := ""
	usedSystem := "kafka"

	// fill commandline parameters in programm variables
	for i := 0; i < len(os.Args); i++ {
		fmt.Printf("%d : %s \n", i, os.Args[i])
		if len(os.Args) > 1{
			if i == 1 {
				usedSystem = os.Args[i]
			}
		}else{
			println("Missing Testsystem !!!")
		}
		if len(os.Args) > 2{
			if i == 2 {
				messagesCMD, err := strconv.ParseInt(os.Args[i], 10, 64)
				if err != nil {
					log.Fatal("%s", err)
				}
				messages = int(messagesCMD)
			}
		}else{
			println("Missing message amount -- using default (1000) !!!")
			messages = 1000
		}

		if len(os.Args) > 3{
			if i == 3 {
				topictemp = os.Args[i]
			}
		}else{
			println("Missing topicname -- using default (test) !!!")
			topictemp = "test"
		}

		if len(os.Args) > 4{
			if i == 4 {
				countprodconCMD, err := strconv.ParseInt(os.Args[i], 10, 64)
				if err != nil {
					log.Fatal("%s", err)
				}
				countprodcon = int(countprodconCMD)
			}
		}else{
			println("Missing Consumer&Producer instances value -- using default (0) !!!")
			countprodcon = 0
		}

		if len(os.Args) > 5{
			if i == 5 {
				if os.Args[i] != "" {
					messageSize = os.Args[i]
				} else {
					fmt.Println("Missing file for binary message (generated file with fixed size of random data)")
					fmt.Println("Sending only the Time and and Identifier for every message !!!")
				}
			}
		}else{
			println("Missing binary message size, only sending Timevalue and 'funny message' !!!")
		}

		if len(os.Args) > 6{
			if i == 6 {
				compressionType = os.Args[i]
			}	
		}else{
			println("Missing compression type, -- using default (json) !!!")
		}
	}

	// start test regarding to the commandline parameters given by the user
	if usedSystem == "kafka"{
		kafka.Kafka(messages, topictemp, countprodcon, compressionType, messageSize)
	}
	if usedSystem == "rabbit"{
		rabbit.Rabbit(messages, topictemp, countprodcon, compressionType, messageSize)
	}

}
