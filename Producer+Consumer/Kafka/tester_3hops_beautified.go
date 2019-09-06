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
var idmapCon = make(map[int]int32, 100)
var finished = make(chan bool, 10000)
var finishedconsumtion = make(chan bool, 10000)
var finishedsending = make(chan bool, 10000)
var finishedprodcon = make(chan bool, 10000)
var finishedprodconend = make(chan bool, 10000)
var brokers []string
var countprodcon int

var messages int
var partitions []string

// var repotchan chan string

func main() {
	messages = 10
	countprodcon = 2
	brokers = []string{"127.0.0.1:9092"}
	partitions = []string{"test1", "test2", "test3", "test4", "test5"}
	// brokers = []string{"127.0.0.1:9001"}

	configEnv()
	starttime := time.Now()
	go producer(1, messages, "test1")

	// for i:=1; i<countprodcon; i++{
	// 	go prodcon(i, messages, partitions[i], partitions[i+1])
	// 	// go prodcon(i, messages, "test2", "test3")
	// }
	go prodconStarter()

	// <- finished
	// go prodcon(2, messages, "test2", "test3", finished, finishedsending, finishedconsumtion)
	go consumer(1, messages, "test1") //--> bei ID=3 gibt es Probleme ????

	// <-finished
	// <-finishedprodcon
	<-finishedsending
	<-finishedconsumtion

	elapsed := time.Since(starttime)
	fmt.Printf("Elapsed time for sending and consuming: %s \nAveragetime per message: %s \n", elapsed, elapsed/time.Duration(messages))

	close(finished)
	close(finishedsending)
	close(finishedconsumtion)
	close(finishedprodcon)

	deleteConfigEnv()

}

func configEnv(){
	brokerAddrs := []string{"localhost:9092"}
    config := sarama.NewConfig()
    config.Version = sarama.V2_1_0_0
	admin, err := sarama.NewClusterAdmin(brokerAddrs, config)

    if err != nil {
        panic(err)
	}
	defer func() { _ = admin.Close() }()
	
	cluster, err := sarama.NewConsumer(brokers, config)

	//get broker
	if err != nil {
		panic(err)
	}
	defer func() {
		if err := cluster.Close(); err != nil {
			panic(err)
		}
	}()

	//get all topic from cluster
	topics, _ := cluster.Topics()

	if contains(topics, "test1") == false{
		err = admin.CreateTopic("test1", &sarama.TopicDetail{
			NumPartitions:     60,
			ReplicationFactor: 1,
		}, false)
		if err != nil {
			panic(err)
		}else{
			// println("Topic created!!!")
		}
	}

	if contains(topics, "test2") == false{
		err = admin.CreateTopic("test2", &sarama.TopicDetail{
			NumPartitions:     60,
			ReplicationFactor: 1,
		}, false)
		if err != nil {
			panic(err)
		}else{
			// println("Topic created!!!")
		}
	}

	if contains(topics, "test3") == false{
		err = admin.CreateTopic("test3", &sarama.TopicDetail{
			NumPartitions:     60,
			ReplicationFactor: 1,
		}, false)
		if err != nil {
			panic(err)
		}else{
			// println("Topic created!!!")
		}
	}
}

func deleteConfigEnv(){
	brokerAddrs := []string{"localhost:9092"}
    config := sarama.NewConfig()
    config.Version = sarama.V2_1_0_0
	admin, err := sarama.NewClusterAdmin(brokerAddrs, config)

    if err != nil {
        panic(err)
	}
	defer func() { _ = admin.Close() }()

	err = admin.DeleteTopic("test1")
	if err != nil {
		panic(err)
	}else{
		// println("Topic deleted!!!")
	}
	err = admin.DeleteTopic("test2")
	if err != nil {
		panic(err)
	}else{
		// println("Topic deleted!!!")
	}
	err = admin.DeleteTopic("test3")
	if err != nil {
		panic(err)
	}else{
		// println("Topic deleted!!!")
	}
}

func producer(producerid int, messages int, targetTopic1 string) {

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

	// brokers := []string{"127.0.0.1:9092"}

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
	topic := targetTopic1

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

		partition, _, err := producer.SendMessage(msg)

		if err != nil {
			panic(err)
		}

		if i < 1 {
			if countprodcon >= producerid{
				setPartitionID(producerid, partition)		
				finished <- true
			}else{
				setPartitionConsumerID(producerid, partition)
				finishedprodconend <- true
			}
			print("Producer sets partitionID: ")
			println(partition)	
		}

		// fmt.Printf("Message %d send to partition %d offset %d \n", i, partition, offset)

	}
	elapsed := time.Since(starttime)
	fmt.Printf("Producer: %d send %d Messages -- elapsed time: %s \nAveragetime per message: %s \n", producerid, sendmessages, elapsed, elapsed/time.Duration(sendmessages))
	finishedsending <- true
	return
}

func consumer(consumerID int, messages int, targetTopic1 string) {
	// for{
	// 	println("Waiting for input on finishedprodcon")
	// 	nachricht, _ := <-finishedprodcon
	// 	if nachricht == true{
	// 		println("leave routine in consumer")
	// 		break
	// 	}
	// }
	<-finishedprodconend

	fmt.Printf("Starting Consumer %d \n", consumerID)
	// we create a configuration structure for our kafka sarama api
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true

	// Specify brokers address. This is default one
	// brokers := []string{"127.0.0.1:9092"}

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
	topic := targetTopic1

	// get partition ID of producer
	partition := getPartitionConsumerID(consumerID)
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

	for i := 0; i < sendmessages; i++ {
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

		// fmt.Printf("got myInfo: %d \n", i)
		// fmt.Print(jsonRecord)
		// fmt.Println()

	}
	elapsed := time.Since(starttime)
	fmt.Printf("Consumer: %d receives %d Messages -- elapsed time: %s \nAveragetime per message: %s \n", consumerID, sendmessages, elapsed, elapsed/time.Duration(sendmessages))
	finishedconsumtion <- true
	return
}

func prodcon(consumerID int, messages int, targetTopic1 string, targetTopic2 string) {
	//contains producer and consumer functionality

	fmt.Printf("Preparing to start ProdCon: %d \n", consumerID)
	if consumerID == 1{
		<-finished
	}else{
		println("lksdhfshfiuisefcmlksajdoishefiuge")
		<- finishedprodcon
	}
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

	// brokers := []string{"127.0.0.1:9092"}

	// my topic
	// topic := targetTopic1
	// *****************************************************************************************************************************************************
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
	var partition int32
	if consumerID > 1 {
		partition = getPartitionID(consumerID-1)//-1 because of setting the partition ID in the process before (consumerID -1)
		fmt.Printf("Consumer + Producer %d get this partition ID: ", consumerID)
		println(partition)
		fmt.Printf("Topic to consume for Consumer + Producer %d: %s \n", consumerID, targetTopic1)
	}else{
		partition = getPartitionID(consumerID)												
		fmt.Printf("Consumer + Producer %d get this partition ID: ", consumerID)
		println(partition)
		fmt.Printf("Topic to consume for Consumer + Producer %d: %s \n", consumerID, targetTopic1)
	}


	// create the consumer on all partitions
	consumerinst, err := masterConsumer.ConsumePartition(targetTopic1, partition, sarama.OffsetOldest) //impotant: partition needs to be the same, the producer pushes to
	if err != nil {
		panic(err)
	}

	fmt.Printf("Start consumer + producer: %d listening to some messages ... please send me something ... \n", consumerID)
	// endless loop, until someone kills me
	sendmessages := messages
	starttime := time.Now()

	for i := 0; i < sendmessages; i++ {
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

		// fmt.Printf("got myInfo number %d on consumer + producer %d \n", i, consumerID)

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

		if i < 1 {

			println("fsijfdbiasflöeshufdisahduil")

			// if countprodcon > consumerID {
			// 	setPartitionID(consumerID, partition)
			// 	finishedprodcon <- true
			// }else{
			// 	setPartitionConsumerID(consumerID, partition)
			// 	finishedprodconend <- true
			// }

			if countprodcon == consumerID{
				setPartitionConsumerID(1, partition)//currently the consumer is hardcoded in this case (there is only one consumer)
				finishedprodconend <- true
			}else{
				finishedprodcon <- true
			}

			fmt.Printf("Consumer + Producer %d sets partitionID: ", consumerID)
			println(partition)
			fmt.Printf("Consumer + Producer %d Topic to send: %s \n", consumerID, targetTopic2)
		}

		// fmt.Printf("Consumer + Producer %d send modified myInfo: %d to topic: %s \n", consumerID, i, targetTopic2)
		// fmt.Print(jsonRecord.ScareMe)
		// println()
		// fmt.Println()

	}
	elapsed := time.Since(starttime)
	fmt.Printf("Consumer + Producer: %d receives and sends %d Messages -- elapsed time: %s \nAveragetime per message: %s \n", consumerID, sendmessages, elapsed, elapsed/time.Duration(sendmessages))
	// finishedsending <- true
}

func prodconStarter(){
	for i:=1; i<=countprodcon; i++{
		fmt.Printf("ProdCon %d in starting process \n", i)
		go prodcon(i, messages, partitions[i-1], partitions[i])
		// go prodcon(i, messages, "test2", "test3")
	}
}

func getPartitionID(prodID int) int32 {
	partition := idmap[prodID]
	return partition
}
func getPartitionConsumerID(prodID int) int32 {
	partition := idmapCon[prodID]
	return partition
}
func setPartitionID(prodID int, partition int32) {
	idmap[prodID] = partition
}
func setPartitionConsumerID(prodID int, partition int32) {
	idmapCon[prodID] = partition
}
func contains(array []string, search string) bool{
	for index := range array {
		if array[index] == search{
			return true
		}
	}
	return false
}

func test() {
	fmt.Println("Testfunction")
	// repotchan <- "Testfunction"

	return
}
