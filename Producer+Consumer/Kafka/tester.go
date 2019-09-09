package main

import (
	"os"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"strconv"
	"time"
	"log"

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
var finishedconsumtion = make(chan bool, 10000)
var finishedsending = make(chan bool, 10000)
var brokers []string
var countprodcon int

var messages int
var partitions []string

var sendTime [10000]string
var consendTime [6][10000]string
var consumeTime [10000]string
var completeTime float64

// var repotchan chan string

func main() {
	topictemp := "test"
	messages = 10
	countprodcon = 2

	// fill commandline parameters in programm variables
	for i:=0; i < len(os.Args); i++{
		fmt.Printf("%d : %s \n", i, os.Args[i])

		if i == 1{
			messagesCMD, err := strconv.ParseInt(os.Args[i], 10,64)
			if err != nil {
				log.Fatal("%s", err)
			}
			messages = int(messagesCMD)
		}
		if i == 2{
			topictemp = os.Args[i]
		}
		if i == 3{
			countprodconCMD, err := strconv.ParseInt(os.Args[i], 10, 64)
			if err != nil {
				log.Fatal("%s", err)
			}
			countprodcon = int(countprodconCMD)
		}
	}
	
	brokers = []string{"127.0.0.1:9092"}
	// partitions = []string{"test1", "test2", "test3", "test4", "test5"}
	// brokers = []string{"127.0.0.1:9001"}

	configEnv(topictemp)
	starttime := time.Now()
	go producer(1, messages, (topictemp + strconv.Itoa(0)), 1)

	go prodconStarter(topictemp)

	// <- finished
	// go prodcon(2, messages, "test2", "test3", finished, finishedsending, finishedconsumtion)
	go consumer(1, messages, (topictemp + strconv.Itoa(countprodcon)), 1) //--> bei ID=3 gibt es Probleme ????

	<-finishedconsumtion

	elapsed := time.Since(starttime)
	fmt.Printf("Elapsed time for sending and consuming: %s \nAveragetime per message: %s \n", elapsed, elapsed/time.Duration(messages))

	// close(finishedsending)
	close(finishedconsumtion)
	
	println("Writing CSV file")
	// write file
	
	f, err := os.Create("testfile.csv")
	if err != nil{
		panic(err)
	}
	defer f.Close()

	f.WriteString("Messagenumber;")
	f.WriteString("ProducerSendTime;")
	f.WriteString("Consumer&ProducerSendTime;")
	f.WriteString("ConsumerTime;\n")

	for i:=0; i < messages; i++{
		// f.WriteString("ProducerSendTime: ")
		f.WriteString(strconv.Itoa(i) + " ;") //Messagenumber
		f.WriteString(sendTime[i] + ";")
		for cp:=1; cp <= countprodcon; cp++{
			// f.WriteString("Consumer&ProducerSendTime: ")
			// f.WriteString(";")
			f.WriteString(consendTime[cp][i])
			if(cp < countprodcon){
				f.WriteString("; \n"  + strconv.Itoa(i) +  "; ;")
			}else{
				f.WriteString(";")
			}
		}
		// f.WriteString("ConsumerTime: ")
		f.WriteString(consumeTime[i])
		f.WriteString("; \n")
	}

	f.WriteString("CompleteTimeDuration;")
	f.WriteString(strconv.FormatFloat(completeTime, 'f', 6, 64))
	f.WriteString("; \n")
	deleteConfigEnv(topictemp)

}

func configEnv(topictemplate string){
	brokerAddrs := []string{"localhost:9092"}
    config := sarama.NewConfig()
	config.Version = sarama.V2_1_0_0
	config.Net.DialTimeout = (1*time.Hour)
	config.Net.WriteTimeout = (1*time.Hour)
	config.Admin.Timeout = (1*time.Hour)
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
	partitiontemp:= topictemplate
		for i:=0; i<=countprodcon; i++{
			if contains(topics, (partitiontemp + strconv.Itoa(i))) == false{
				err = admin.CreateTopic((partitiontemp + strconv.Itoa(i)), &sarama.TopicDetail{
					NumPartitions:     60,
					ReplicationFactor: 1,
				}, false)
				if err != nil {
					panic(err)
				}else{
					fmt.Printf("Topic %s created!!! \n", (partitiontemp + strconv.Itoa(i)))
				}
			}
		}
}

func deleteConfigEnv(topictemplate string){
	brokerAddrs := []string{"localhost:9092"}
    config := sarama.NewConfig()
	config.Version = sarama.V2_1_0_0
	config.Net.DialTimeout = (1*time.Hour)
	config.Net.WriteTimeout = (1*time.Hour)
	config.Admin.Timeout = (1*time.Hour)
	admin, err := sarama.NewClusterAdmin(brokerAddrs, config)

    if err != nil {
        panic(err)
	}
	defer func() { _ = admin.Close() }()

	partitiontemp:= topictemplate
	for i:=0; i<=countprodcon; i++{
			err = admin.DeleteTopic((partitiontemp + strconv.Itoa(i)))
			if err != nil {
				panic(err)
			}else{
				fmt.Printf("Topic %s deleted!!! \n", (partitiontemp + strconv.Itoa(i)))
			}
	}
}

func producer(producerid int, messages int, targetTopic1 string, targetPartition int32) {

	fmt.Printf("Starting Producer %d \n", producerid)
	//	segmenthelper.LogInit("experimental.kafka-producer", "experimental", "test")

	// Setup configuration
	config := sarama.NewConfig()
	// config.Timeout := --> malsehen ?!?!?

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

	config.Producer.Partitioner = sarama.NewManualPartitioner //set the partition amnually siplyifes the test (no complex get and set partition!!!)

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
	partition := targetPartition

	fmt.Printf("Start producer: %d by sending a creepy and scary message \n", producerid)

	starttime := time.Now()

	sendmessages := messages

	fmt.Printf("Producer %d Topic to send: %s \n", producerid, targetTopic1)
	fmt.Printf("Producer %d sets partitionID: %d \n", producerid, targetPartition)

	var testifleinput []byte
	var jsonMsg MyInfo

	if testifleinput == nil {
		testifleinput, err = ioutil.ReadFile("../../output-1Kibi-rand")
		if err != nil {
			fmt.Print(err)
		}
	}

	jsonMsg.ScareMe = "Yes, please"
	jsonMsg.Binaryfile = testifleinput

	for i := 0; i < sendmessages; i++ {

		messageStartTime := time.Now()

		jsonMsg.TheTime = strconv.Itoa(int(time.Now().UnixNano()))

		jsonOutput, _ := json.Marshal(&jsonMsg)
		jsonString := (string)(jsonOutput)

		msg := &sarama.ProducerMessage{
			Topic: topic,
			Key:   sarama.StringEncoder("myInfo"),
			Value: sarama.StringEncoder(jsonString),
			Partition: partition,
		}

		// fmt.Println("Sending Message : ")
		// fmt.Println(msg)

		_, _, err := producer.SendMessage(msg)

		if err != nil {
			println(len(jsonString))
			panic(err)
		}

		messageEndTime:= time.Since(messageStartTime).Seconds()*1000
		sendTime[i] = strconv.FormatFloat(messageEndTime, 'f', 6, 64)
		completeTime = completeTime + messageEndTime
		// fmt.Printf("Message %d send to partition %d offset %d \n", i, partition, offset)

	}
	elapsed := time.Since(starttime)
	fmt.Printf("Producer: %d send %d Messages -- elapsed time: %s \nAveragetime per message: %s \n", producerid, sendmessages, elapsed, elapsed/time.Duration(sendmessages))
	// finishedsending <- true
	return
}

func consumer(consumerID int, messages int, targetTopic1 string, targetPartition int32) {

	fmt.Printf("Starting Consumer %d \n", consumerID)
	fmt.Printf("Consumer %d Topic to consume: %s \n",consumerID, targetTopic1)
	fmt.Printf("Consumer %d get partitionID: %d \n", consumerID, targetPartition)
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
	partition := targetPartition

	// create the consumer on all partitions
	consumer, err := master.ConsumePartition(topic, partition, sarama.OffsetOldest) //impotant: partition needs to be the same, the producer pushes to
	if err != nil {
		panic(err)
	}

	fmt.Printf("Start consumer: %d listening to some messages ... please send me something ... \n", consumerID)
	// endless loop, until someone kills me
	sendmessages := messages
	starttime := time.Now()

	// read the message
	msg := <-consumer.Messages()

	for i := 0; i < sendmessages; i++ {
		// fmt.Print(" ... waiting for message ...")
		messageReceivedTime := time.Now().UnixNano()	// --> wenn die Zeit hier genommen wird, wird die Laufzeit der Forschleife mit eingerechnet

		keyString := string(msg.Key)

		if keyString != "myInfo" {
			fmt.Println("received key is not myInfo ... sorry ... message ignore")
			continue
		}

		var jsonRecord MyInfo
		json.Unmarshal(msg.Value, &jsonRecord)

		timevalue, err := strconv.ParseInt(jsonRecord.TheTime, 10, 64)
		if err != nil {
			log.Fatal("%s", err)
		}

		duration:= messageReceivedTime - timevalue
		durationMs := float64(duration) / float64(1000000) //Nanosekunden in Milisekunden

		consumeTime[i] = strconv.FormatFloat(durationMs, 'f', 6, 64)
		completeTime = completeTime + durationMs	 

		// fmt.Printf("got myInfo: %d \n", i)
		// fmt.Print(jsonRecord)
		// fmt.Println()

		// messageEndTime:= time.Since(messageStartTime).Seconds()*1000
		// consumeTime[i] = strconv.FormatFloat(messageEndTime, 'f', 6, 64)
		// completeTime = completeTime + messageEndTime
	}
	elapsed := time.Since(starttime)
	fmt.Printf("Consumer: %d receives %d Messages -- elapsed time: %s \nAveragetime per message: %s \n", consumerID, sendmessages, elapsed, elapsed/time.Duration(sendmessages))
	finishedconsumtion <- true
	return
}

func prodcon(consendID int, messages int, targetTopic1 string, conPartition int32, targetTopic2 string, targetPartition int32) {
	//contains producer and consumer functionality

	fmt.Printf("Starting Producer with Consumer %d \n", consendID)
	fmt.Printf("Consumer + Producer %d Topic to consume: %s \n", consendID, targetTopic1)
	fmt.Printf("Consumer + Producer %d get partitionID: %d \n", consendID, conPartition)

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

	configProducer.Producer.Partitioner = sarama.NewManualPartitioner //set the partition amnually siplyifes the test (no complex get and set partition!!!)

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
	partition := conPartition


	// create the consumer on all partitions
	consumerinst, err := masterConsumer.ConsumePartition(targetTopic1, partition, sarama.OffsetOldest) //impotant: partition needs to be the same, the producer pushes to
	if err != nil {
		panic(err)
	}

	fmt.Printf("Start consumer + producer: %d listening to some messages ... please send me something ... \n", consendID)
	// endless loop, until someone kills me
	sendmessages := messages
	starttime := time.Now()

	// read the message
	msg := <-consumerinst.Messages()

	for i := 0; i < sendmessages; i++ {
		// fmt.Print(" ... waiting for message ...")
		messageReceivedTime := time.Now().UnixNano()	// --> wenn die Zeit hier genommen wird, wird die Laufzeit der Forschleife mit eingerechnet

		keyString := string(msg.Key)

		if keyString != "myInfo" {
			fmt.Println("received key is not myInfo ... sorry ... message ignore")
			continue
		}

		var jsonRecord MyInfo

		json.Unmarshal(msg.Value, &jsonRecord)

		timevalue, err := strconv.ParseInt(jsonRecord.TheTime, 10, 64)
		if err != nil {
			log.Fatal("%s", err)
		}

		duration:= messageReceivedTime - timevalue
		durationMs := float64(duration) / float64(1000000) //Nanosekunden in Milisekunden

		consendTime[consendID][i] = strconv.FormatFloat(durationMs, 'f', 6, 64)
		completeTime = completeTime + durationMs	 


		// fmt.Printf("got myInfo number %d on consumer + producer %d \n", i, consumerID)

		jsonRecord.ScareMe = jsonRecord.ScareMe + strconv.Itoa(consendID)
		jsonRecord.TheTime = strconv.Itoa(int(time.Now().UnixNano()))

		jsonOutput, _ := json.Marshal(jsonRecord)
		jsonString := (string)(jsonOutput)

		msgout := &sarama.ProducerMessage{
			Topic: targetTopic2,
			Key:   sarama.StringEncoder("myInfo"),
			Value: sarama.StringEncoder(jsonString),
			Partition: targetPartition,
		}

		// fmt.Println("Sending Message : ")
		// fmt.Println(msg)
		messageStartTime := time.Now()

		partition, _, err := producerInst.SendMessage(msgout)

		if err != nil {
			panic(err)
		}

		messageEndTime:= time.Since(messageStartTime).Seconds()*1000
		consendTime[consendID][i] = strconv.FormatFloat((durationMs + messageEndTime), 'f', 6, 64)
		completeTime = completeTime + messageEndTime

		if i < 1{
			fmt.Printf("Consumer + Producer %d sets partitionID: ", consendID)
			println(partition)
			fmt.Printf("Consumer + Producer %d Topic to send: %s \n", consendID, targetTopic2)
		// }
		}

		// fmt.Printf("Consumer + Producer %d send modified myInfo: %d to topic: %s \n", consumerID, i, targetTopic2)
		// fmt.Print(jsonRecord.ScareMe)

		// messageEndTime:= time.Since(messageStartTime).Seconds()*1000
		// consendTime[consendID][i] = strconv.FormatFloat(messageEndTime, 'f', 6, 64)
		// completeTime = completeTime + messageEndTime

	}
	elapsed := time.Since(starttime)
	fmt.Printf("Consumer + Producer: %d receives and sends %d Messages -- elapsed time: %s \nAveragetime per message: %s \n", consendID, sendmessages, elapsed, elapsed/time.Duration(sendmessages))
}

func prodconStarter(topictemplate string){
	topictemp := topictemplate
	for i:=1; i <= countprodcon; i++{
		fmt.Printf("ProdCon %d in starting process \n", i)
		go prodcon(i, messages, (topictemp + strconv.Itoa(i-1)), 1, (topictemp + strconv.Itoa(i)), 1)
	}
}

func contains(array []string, search string) bool{
	for index := range array {
		if array[index] == search{
			return true
		}
	}
	return false
}