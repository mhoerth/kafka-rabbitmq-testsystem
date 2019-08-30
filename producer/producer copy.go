package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"strconv"
	"time"

	/*  "../../src/mdp/segmenthelper"
	 */
	"github.com/Shopify/sarama"
) 

func main() {

	//	segmenthelper.LogInit("experimental.kafka-producer", "experimental", "test")

	// Setup configuration
	config := sarama.NewConfig()

	// Return specifies what channels will be populated.
	// If they are set to true, you must read from
	// config.Producer.Return.Successes = true
	// The total number of times to retry sending a message (default 3).
	config.Producer.Retry.Max = 5

	// The level of acknowledgement reliability needed from the broker.
	config.Producer.RequiredAcks = sarama.WaitForAll

	// for sync
	config.Producer.Return.Successes = true
	//for batching ???
	config.Producer.Flush.MaxMessages = 1

	brokers := []string{"127.0.0.1:19092"}

	// create a producer
	producer, err := sarama.NewSyncProducer(brokers, config)
	if err != nil {
		panic(err)
	}

	defer func() {
		if err := producer.Close(); err != nil {
			panic(err)
		}
	}()

	// my topic
	topic := "testtopic"

	fmt.Println("Start the producer by sending a creepy and scary message")

	type MyInfo struct {
		TheTime    string `json:"theTime"`
		ScareMe    string `json:"scare"`
		Binaryfile []byte `json:"binary"`
	}
	for {
		var testifleinput []byte
		var jsonMsg MyInfo

		if testifleinput == nil {
			testifleinput, err = ioutil.ReadFile("../output-100Kibi")
			if err != nil {
				fmt.Print(err)
			}
		}
		// fmt.Print(testifleinput)
		jsonMsg.TheTime = strconv.Itoa(int(time.Now().Unix()))
		jsonMsg.ScareMe = "Yes, please"
		jsonMsg.Binaryfile = testifleinput

		jsonOutput, _ := json.Marshal(&jsonMsg)
		jsonString := (string)(jsonOutput)

		msg := &sarama.ProducerMessage{
			Topic: topic,
			Key:   sarama.StringEncoder("myInfo"),
			Value: sarama.StringEncoder(jsonString),
		}

		fmt.Println("Sending Message : ")
		fmt.Println(msg)

		partition, offset, err := producer.SendMessage(msg)

		if err != nil {
			panic(err)
		}

		fmt.Printf("Message send to partition %d offset %d", partition, offset)

		//segmenthelper.LogDebug("Yah a message for the logging bus")

		fmt.Println(" ... now sleep a moment ...")

		// time.Sleep(10000000000) // 10 sec
	}
}
