package main

import (
	"fmt"
	"log"
	"os"
	"strconv"

	"mom-test/kafka"
	"mom-test/rabbit"
)

func main() {

	// set default values to avoid programm blocking if no parameter is set by the user
	topictemp := "test"
	messages := 1
	countprodcon := 0
	compressionType := "json"
	messageSize := "testfiles/output-1Kibi-rand-baseEnc"
	usedSystem := "kafka"
	interations := 1
	delayTime := 100
	synchronicity := "async"

	// testfiles
	Kibi1 := "testfiles/output-1Kibi-rand-baseEnc"
	Kibi10 := "testfiles/output-10Kibi-rand-baseEnc"
	Kibi100 := "testfiles/output-100Kibi-rand-baseEnc"
	Kibi500 := "testfiles/output-500Kibi-rand-baseEnc"
	Kibi750 := "testfiles/output-750Kibi-rand-baseEnc"
	Kibi960 := "testfiles/output-960Kibi-rand-baseEnc"

	// fill commandline parameters in programm variables
	for i := 0; i < len(os.Args); i++ {
		fmt.Printf("%d : %s \n", i, os.Args[i])
		
		if len(os.Args) > 1{
			if i == 1 {
				interationsCMD, err := strconv.ParseInt(os.Args[i], 10, 64)
				if err != nil {
					log.Fatal("%s", err)
				}
				interations = int(interationsCMD)
			}
		}else{
			println(" Missing interation amount, dafault is 1")
		}
		
		if len(os.Args) > 2{
			if i == 2 {
				usedSystem = os.Args[i]
			
				if os.Args[i] == "asynckafka"{
					synchronicity = "async"
					println("using async")
				}else if os.Args[i] == "synckafka"{
					synchronicity = "sync"
					println("using sync")
				}else{
					println("using default , synchronicity --> async")
				}
			}
		}else{
			println("Using default Testsystem --> Kafka !!!")
		}
		if len(os.Args) > 3{
			if i == 3 {
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

		if len(os.Args) > 4{
			if i == 4 {
				delayTimeCMD, err := strconv.ParseInt(os.Args[i], 10, 64)
				if err != nil {
					log.Fatal("%s", err)
				}
				delayTime = int(delayTimeCMD)
			}	
		}else{
			println("Missing delayTime in ms between messages, -- using default (100 ms) !!!")
		}

		if len(os.Args) > 5{
			if i == 5 {
				topictemp = os.Args[i]
			}
		}else{
			println("Missing topicname -- using default (test) !!!")
			topictemp = "test"
		}

		if len(os.Args) > 6{
			if i == 6 {
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

		if len(os.Args) > 7{
			if i == 7 {
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

		if len(os.Args) > 8{
			if i == 8 {
				compressionType = os.Args[i]
				
			}else{
				println("Missing compression type, -- using default (json) !!!")
			}
		}

	}

	// start test regarding to the commandline parameters given by the user
	if usedSystem == "test"{
		// test kafka
		kafka.Kafka(interations, messages, topictemp, countprodcon, compressionType, Kibi1, delayTime, synchronicity)
		kafka.Kafka(interations, messages, topictemp, countprodcon, compressionType, Kibi10, delayTime, synchronicity)
		kafka.Kafka(interations, messages, topictemp, countprodcon, compressionType, Kibi100, delayTime, synchronicity)
		kafka.Kafka(interations, messages, topictemp, countprodcon, compressionType, Kibi500, delayTime, synchronicity)
		kafka.Kafka(interations, messages, topictemp, countprodcon, compressionType, Kibi750, delayTime, synchronicity)
		kafka.Kafka(interations, messages, topictemp, countprodcon, compressionType, Kibi960, delayTime, synchronicity)
		kafka.CloseChannels()
		// test rabbitmq
		rabbit.Rabbit(interations, messages, topictemp, countprodcon, compressionType, Kibi1, delayTime)
		rabbit.Rabbit(interations, messages, topictemp, countprodcon, compressionType, Kibi10, delayTime)
		rabbit.Rabbit(interations, messages, topictemp, countprodcon, compressionType, Kibi100, delayTime)
		rabbit.Rabbit(interations, messages, topictemp, countprodcon, compressionType, Kibi500, delayTime)
		rabbit.Rabbit(interations, messages, topictemp, countprodcon, compressionType, Kibi750, delayTime)
		rabbit.Rabbit(interations, messages, topictemp, countprodcon, compressionType, Kibi960, delayTime)
		rabbit.CloseChannels()
	}

	if usedSystem == "kafka" || usedSystem == "synckafka" || usedSystem == "asynckafka"{
		if messageSize == "test"{
			kafka.Kafka(interations, messages, topictemp, countprodcon, compressionType, Kibi1, delayTime, synchronicity)
			kafka.Kafka(interations, messages, topictemp, countprodcon, compressionType, Kibi10, delayTime, synchronicity)
			kafka.Kafka(interations, messages, topictemp, countprodcon, compressionType, Kibi100, delayTime, synchronicity)
			kafka.Kafka(interations, messages, topictemp, countprodcon, compressionType, Kibi500, delayTime, synchronicity)
			kafka.Kafka(interations, messages, topictemp, countprodcon, compressionType, Kibi750, delayTime, synchronicity)
			kafka.Kafka(interations, messages, topictemp, countprodcon, compressionType, Kibi960, delayTime, synchronicity)
			kafka.CloseChannels()
		}else{
			kafka.Kafka(interations, messages, topictemp, countprodcon, compressionType, messageSize, delayTime, synchronicity)
			rabbit.CloseChannels()
		}
	}
	if usedSystem == "rabbit"{
		if messageSize == "test"{
			rabbit.Rabbit(interations, messages, topictemp, countprodcon, compressionType, Kibi1, delayTime)
			rabbit.Rabbit(interations, messages, topictemp, countprodcon, compressionType, Kibi10, delayTime)
			rabbit.Rabbit(interations, messages, topictemp, countprodcon, compressionType, Kibi100, delayTime)
			rabbit.Rabbit(interations, messages, topictemp, countprodcon, compressionType, Kibi500, delayTime)
			rabbit.Rabbit(interations, messages, topictemp, countprodcon, compressionType, Kibi750, delayTime)
			rabbit.Rabbit(interations, messages, topictemp, countprodcon, compressionType, Kibi960, delayTime)
			rabbit.CloseChannels()
		}else{
			rabbit.Rabbit(interations, messages, topictemp, countprodcon, compressionType, messageSize, delayTime)
			rabbit.CloseChannels()
		}
	}

}
