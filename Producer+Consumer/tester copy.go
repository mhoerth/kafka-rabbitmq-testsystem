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

type MyInfo struct {
	TheTime    string `json:"theTime"`
	ScareMe    string `json:"scare"`
	Binaryfile []byte `json:"binary"`
}

// var idmap map[int]int32
var idmap = make(map[int]int32, 100)

// var repotchan chan string

func main() {
	messages := 100
	// idmap := make(map[int]int32, 100)
	finished := make(chan bool, 10000)
	finishedconsumtion := make(chan bool, 10000)
	finishedsending := make(chan bool, 10000)
	// idmap[0] = 0
	// 	repotchan := make(chan string, 5)
	// 	// test()
	// 	println("before go routine")
	// 	go test()
	// 	println("before for statement")
	// 	for i:=0; i<=1; i++{
	// 	msg := <-repotchan
	// 	println(msg)
	// 	println("directly after channel print")
	// }
	// 	close(repotchan)
	// 	println("after channel close")

	starttime := time.Now()
	// go producer(1, messages)
	// go consumer(1)
	go producer(1, messages, finished, finishedsending)
	<- finished
	// go producer(2, messages, finished, finishedsending)
	// <- finished
	// go producer(3, messages, finished, finishedsending)
	// go producer(4, messages, finished, finishedsending)
	go consumer(1, messages, finishedconsumtion)
	// go consumer(2, messages, finishedconsumtion)    //--> muss noch einbauen, dass ich vor dem Messen der Zeit audf alle consumer warten muss!!!
	// go consumer(3, messages, finishedconsumtion)
	// go consumer(4, messages, finishedconsumtion)
	// for i := 0; i < 1; i++ {
	// 	<-finishedconsumtion
	// 	// <- finishedsending
	// }
	<- finishedsending
	<- finishedconsumtion
	close(finished)
	close(finishedsending)
	close(finishedconsumtion)
	elapsed := time.Since(starttime)
	fmt.Printf("Elapsed time for sending and consuming: %s \nAveragetime per message: %s \n", elapsed, elapsed/time.Duration(messages))

}

func producer(producerid int, messages int, finished chan bool, finishedsending chan bool) {
	// if producerid > 1{
	// 	for i := 1; i < producerid; i++ {
	// 		<- finished
	// 	}
	// }

	// if producerid > 1{
	// 	<- finished
	// }

	fmt.Printf("Starting Producer %d \n", producerid)
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

	brokers := []string{"127.0.0.1:9092"}

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
	topic := "test"

	fmt.Printf("Start producer: %d by sending a creepy and scary message \n", producerid)

	starttime := time.Now()

	sendmessages := messages

	for i := 0; i < sendmessages; i++ {
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

		// fmt.Println("Sending Message : ")
		// fmt.Println(msg)

		partition, offset, err := producer.SendMessage(msg)

		if err != nil {
			panic(err)
		}

		setPartitionID(producerid, partition)
		print("Producer sets partitionID: ")
		println(partition)
		finished <- true
		fmt.Printf("Message %d send to partition %d offset %d \n", i, partition, offset)

		//segmenthelper.LogDebug("Yah a message for the logging bus")

		// fmt.Println(" ... now sleep a moment ...")

		// time.Sleep(10000000000) // 10 sec
	}
	elapsed := time.Since(starttime)
	fmt.Printf("Producer: %d send %d Messages -- elapsed time: %s \nAveragetime per message: %s \n", producerid, sendmessages, elapsed, elapsed/time.Duration(sendmessages))
	finishedsending <- true
	return
}

func consumer(consumerID int, messages int, finishedconsumtion chan bool) {
	fmt.Printf("Starting Consumer %d \n", consumerID)
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

	// get partition ID of producer
	partition := getPartitionID(consumerID)
	fmt.Printf("Consumer %d get this partition ID: ", consumerID)
	println(partition)

	// create the consumer on all partitions
	consumer, err := master.ConsumePartition(topic, partition, sarama.OffsetOldest) //impotant: partition needs to be the same, the producer pushes to
	if err != nil {
		panic(err)
	}

	fmt.Printf("Start consumer: %d listening to some messages ... please send me something ... \n", consumerID)
	// endless loop, until someone kills me
	sendmessages := messages
	for i := 0; i <= sendmessages; i++ {
		// fmt.Print(" ... waiting for message ...")

		// read the message
		msg := <-consumer.Messages()

		keyString := string(msg.Key)

		if keyString != "myInfo" {
			fmt.Println("received key is not myInfo ... sorry ... message ignore")
			continue
		}

		var jsonRecord MyInfo

		json.Unmarshal(msg.Value, &jsonRecord)

		fmt.Printf("got myInfo: %d", i)
		// fmt.Print(jsonRecord)
		// fmt.Println()

	}
	finishedconsumtion <- true
	return
}

func getPartitionID(prodID int) int32 {
	partition := idmap[prodID]
	return partition
}
func setPartitionID(prodID int, partition int32) {
	idmap[prodID] = partition
}

func test() {
	fmt.Println("Testfunction")
	// repotchan <- "Testfunction"

	return
}
