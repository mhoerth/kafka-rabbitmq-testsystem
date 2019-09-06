package main

import (
	"encoding/json"
	"fmt"

	"github.com/Shopify/sarama"
)

func main() {

	// we create a configuration structure for our kafka sarama api
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true

	// Specify brokers address. This is default one
	brokers := []string{"127.0.0.1:9092"}

	// Create new consumer
	master, err := sarama.NewConsumer(brokers, config)
	if err != nil {
		panic(err)
	}

	defer func() {
		if err := master.Close(); err != nil {
			panic(err)
		}
	}()

	// the topic where we want to listen at
	topic := "test"

	// create the consumer on all partitions
	consumer, err := master.ConsumePartition(topic, 17, sarama.OffsetOldest) //impotant: partition needs to be the same, the producer pushes to
	if err != nil {
		panic(err)
	}

	fmt.Println("Start listening to some messages ... please send me something ...")

	type MyInfo struct {
		TheTime    string `json:"theTime"`
		ScareMe    string `json:"scare"`
		Binaryfile []byte   `json:"binary"`
	}

	// endless loop, until someone kills me
	for {
		fmt.Print(" ... waiting for message ...")

		// read the message
		msg := <-consumer.Messages()

		keyString := string(msg.Key)

		if keyString != "myInfo" {
			fmt.Println("received key is not myInfo ... sorry ... message ignore")
			continue
		}

		var jsonRecord MyInfo

		json.Unmarshal(msg.Value, &jsonRecord)

		fmt.Print("got myInfo:")
		fmt.Print(jsonRecord)
		fmt.Println()

	}
}
