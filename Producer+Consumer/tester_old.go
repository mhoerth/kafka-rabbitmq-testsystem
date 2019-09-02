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
	messages := 10
	finished := make(chan bool, 10000)
	finishedconsumtion := make(chan bool, 10000)
	finishedsending := make(chan bool, 10000)

	starttime := time.Now()
	go prodcon(1, messages, "test1", "test2", finished, finishedsending, finishedconsumtion)
	// <- finished
	// go prodcon(2, messages, "test2", "test3", finished, finishedsending, finishedconsumtion)
	go prodcon(2, messages, "test2", "", finished, finishedsending, finishedconsumtion) //--> bei ID=3 gibt es Probleme ????

	<-finished
	<-finishedsending
	<-finishedconsumtion
	close(finished)
	close(finishedsending)
	close(finishedconsumtion)
	elapsed := time.Since(starttime)
	fmt.Printf("Elapsed time for sending and consuming: %s \nAveragetime per message: %s \n", elapsed, elapsed/time.Duration(messages))

}

func producer(producerid int, messages int, finished chan bool, finishedsending chan bool) {

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
			testifleinput, err = ioutil.ReadFile("../output-1Kibi")
			if err != nil {
				fmt.Print(err)
			}
		}
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
	starttime := time.Now()

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

		fmt.Printf("got myInfo: %d \n", i)
		// fmt.Print(jsonRecord)
		// fmt.Println()

	}
	elapsed := time.Since(starttime)
	fmt.Printf("Consumer: %d receives %d Messages -- elapsed time: %s \nAveragetime per message: %s \n", consumerID, sendmessages, elapsed, elapsed/time.Duration(sendmessages))
	finishedconsumtion <- true
	return
}

func prodcon(consumerID int, messages int, targetTopic1 string, targetTopic2 string, finished chan bool, finishedsending chan bool, finishedconsumtion chan bool) {
	//contains producer and consumer functionality

	fmt.Printf("Starting Producer with Consumer %d \n", consumerID)
	//	segmenthelper.LogInit("experimental.kafka-producer", "experimental", "test")

	// Setup configuration
	configProducer := sarama.NewConfig()
	configConsumer := sarama.NewConfig()

	// Return specifies what channels will be populated.
	// If they are set to true, you must read from
	// config.Producer.Return.Successes = true
	// The total number of times to retry sending a message (default 3).
	configProducer.Producer.Retry.Max = 5

	// The level of acknowledgement reliability needed from the broker.
	configProducer.Producer.RequiredAcks = sarama.WaitForAll

	// for sync
	configProducer.Producer.Return.Successes = true
	//for batching ???
	configProducer.Producer.Flush.MaxMessages = 1
	//config consumer
	configConsumer.Consumer.Return.Errors = true

	brokers := []string{"127.0.0.1:9092"}

	// my topic
	topic := targetTopic1
	// *****************************************************************************************************************************************************
	if consumerID <= 1 {
		//Producer Part
		// create a producer
		println("Starting pure producer")
		producerInst, err := sarama.NewSyncProducer(brokers, configProducer)
		if err != nil {
			panic(err)
		}

		defer func() {
			if err := producerInst.Close(); err != nil {
				panic(err)
			}
		}()

		fmt.Printf("Start producer: %d by sending a creepy and scary message to topic: %s \n", consumerID, topic)

		starttime := time.Now()

		sendmessages := messages

		for i := 0; i < sendmessages; i++ {
			var testifleinput []byte
			var jsonMsg MyInfo

			if testifleinput == nil {
				testifleinput, err = ioutil.ReadFile("../output-1Kibi")
				if err != nil {
					fmt.Print(err)
				}
			}
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

			partition, offset, err := producerInst.SendMessage(msg)

			if err != nil {
				panic(err)
			}

			if i == 0 {
				setPartitionID(consumerID, partition)
				print("Producer sets partitionID: ")
				println(partition)
				finished <- true
				fmt.Printf("Producer %d Topic to send: %s \n", consumerID, topic)
			}

			fmt.Printf("Message %d send to partition %d offset %d \n", i, partition, offset)

		}
		elapsed := time.Since(starttime)
		fmt.Printf("Producer: %d send %d Messages -- elapsed time: %s \nAveragetime per message: %s \n", consumerID, sendmessages, elapsed, elapsed/time.Duration(sendmessages))
		finishedsending <- true
	}
	// *****************************************************************************************************************************************************
	if targetTopic2 != "" {
		//Consumer + Producer Part
		// Create new consumer + producer with modified message
		println("Starting Consumer + Producer !!!")
		masterConsumer, err := sarama.NewConsumer(brokers, configConsumer)
		if err != nil {
			panic(err)
		}

		defer func() {
			if err := masterConsumer.Close(); err != nil {
				panic(err)
			}
		}()

		producerInst, err := sarama.NewSyncProducer(brokers, configProducer)
		if err != nil {
			panic(err)
		}

		defer func() {
			if err := producerInst.Close(); err != nil {
				panic(err)
			}
		}()

		// the topic where we want to listen at
		// topic := targetTopic2
		// fmt.Printf("targetTopic1: %s \n", targetTopic1)

		// get partition ID of producer
		partition := getPartitionID(consumerID)
		fmt.Printf("Consumer + Producer %d get this partition ID: ", consumerID)
		println(partition)
		fmt.Printf("Topic to consume for Consumer + Producer %d: %s \n", consumerID, targetTopic1)
		println("blatest")

		// create the consumer on all partitions
		consumerinst, err := masterConsumer.ConsumePartition(targetTopic1, partition, sarama.OffsetOldest) //impotant: partition needs to be the same, the producer pushes to
		if err != nil {
			panic(err)
		}

		fmt.Printf("Start consumer + producer: %d listening to some messages ... please send me something ... \n", consumerID)
		// endless loop, until someone kills me
		sendmessages := messages
		starttime := time.Now()

		for i := 0; i <= sendmessages; i++ {
			// fmt.Print(" ... waiting for message ...")

			// read the message
			msg := <-consumerinst.Messages()

			keyString := string(msg.Key)

			if keyString != "myInfo" {
				fmt.Println("received key is not myInfo ... sorry ... message ignore")
				continue
			}

			var jsonRecord MyInfo

			json.Unmarshal(msg.Value, &jsonRecord)

			fmt.Printf("got myInfo number %d on consumer + producer %d \n", i, consumerID)

			jsonRecord.ScareMe = jsonRecord.ScareMe + strconv.Itoa(consumerID)
			jsonRecord.TheTime = strconv.Itoa(int(time.Now().Unix()))

			jsonOutput, _ := json.Marshal(jsonRecord)
			jsonString := (string)(jsonOutput)

			msgout := &sarama.ProducerMessage{
				Topic: targetTopic2,
				Key:   sarama.StringEncoder("myInfo"),
				Value: sarama.StringEncoder(jsonString),
			}

			// fmt.Println("Sending Message : ")
			// fmt.Println(msg)

			partition, _, err := producerInst.SendMessage(msgout)

			if err != nil {
				panic(err)
			}

			if i == 0 {
				setPartitionID(consumerID, partition)
				fmt.Printf("Consumer + Producer %d sets partitionID: ", consumerID)
				println(partition)
				finished <- true
				fmt.Printf("Consumer + Producer %d Topic to send: %s \n", consumerID, targetTopic2)
			}

			fmt.Printf("Consumer + Producer %d send modified myInfo: %d to topic: %s \n", consumerID, i, topic)
			// fmt.Print(jsonRecord.ScareMe)
			// println()
			// fmt.Println()

		}
		elapsed := time.Since(starttime)
		fmt.Printf("Consumer + Producer: %d receives and sends %d Messages -- elapsed time: %s \nAveragetime per message: %s \n", consumerID, sendmessages, elapsed, elapsed/time.Duration(sendmessages))
		finishedsending <- true
	}
	// *******************************************************************************************************************************************************
	if targetTopic2 == "" {
		//Only Consumer !!! (last one in the chain)
		// the topic where we want to listen at

		// for i := 0; i <= 2; i++ {
		// 	<-finished
		// }
		<- finished

		println("Starting pure Consumer")
		masterConsumer, err := sarama.NewConsumer(brokers, configConsumer)
		if err != nil {
			panic(err)
		}

		defer func() {
			if err := masterConsumer.Close(); err != nil {
				panic(err)
			}
		}()

		// the topic where we want to listen at
		topic := targetTopic1

		// get partition ID of producer
		partition := getPartitionID(consumerID -1) // consumerID-1, because of mapping the consumeer topic with the producer topic
		fmt.Printf("Consumer %d get this partition ID: ", consumerID)
		println(partition)
		fmt.Printf("Consumer %d Topic to consume: %s \n", consumerID, topic)

		// create the consumer on all partitions
		consumer, err := masterConsumer.ConsumePartition(topic, partition, sarama.OffsetOldest) //impotant: partition needs to be the same, the producer pushes to
		if err != nil {
			panic(err)
		}

		fmt.Printf("Start consumer: %d listening to some messages ... please send me something ... \n", consumerID)
		// endless loop, until someone kills me
		sendmessages := messages
		starttime := time.Now()

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

			fmt.Printf("got myInfo: %d \n", i)
			fmt.Print(jsonRecord.ScareMe)
			println()
			// fmt.Println()

		}
		elapsed := time.Since(starttime)
		fmt.Printf("(Last) Consumer: %d receives %d Messages -- elapsed time: %s \nAveragetime per message: %s \n", consumerID, sendmessages, elapsed, elapsed/time.Duration(sendmessages))
		finishedconsumtion <- true
	}
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