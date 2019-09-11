package main

import (
	// "encoding/binary"
	"context"
	"os"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"strconv"
	"time"
	"log"

	// "bufio"
	"bytes"
	// "encoding/gob"

	/*  "../../src/mdp/segmenthelper"
	 */
	"github.com/Shopify/sarama"
	"github.com/linkedin/goavro"
)

type MyInfo struct {
	TheTime    string `json:"theTime"`
	ScareMe    string `json:"scareme"`
	Binaryfile []byte `json:"binaryfile"`
}
// type MyInfo2 struct {
// 	Binaryfile []byte `json:"binary"`
// 	ScareMe    string `json:"scare"`
// 	TheTime    string `json:"theTime"`
// }

// var idmap map[int]int32
var finishedconsumtion = make(chan bool, 10000)
var finishedsending = make(chan bool, 10000)
var finishedgroup = make(chan bool, 10000)
var brokers []string
var countprodcon int

var messages int
var partitions []string

var sendTime [10000]string
var consendTime [6][10000]string
var consumeTime [10000]string
var completeTime float64
var sessionStarttime int64

// var repotchan chan string
var testing int
// var m map[[]byte][string][string] interface{}

// for avro
const loginEventAvroSchema = `{"type": "record", "name": "LoginEvent", "fields": [{"name": "TheTime", "type": "string"}, {"name": "ScareMe", "type": "string"}, {"name": "Binaryfile", "type": "bytes"}]}`
var compressionType string

func main() {
	topictemp := "test"
	messages = 1
	countprodcon = 0
	compressionType = ""
	// avro test

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
		if i == 4{
			compressionType = os.Args[i]
		}
	}
	
	brokers = []string{"127.0.0.1:9092"}
	// partitions = []string{"test1", "test2", "test3", "test4", "test5"}
	// brokers = []string{"127.0.0.1:9001"}
	configEnv(topictemp)
	starttime := time.Now()
	go producer(1, messages, (topictemp + strconv.Itoa(0)), 1)
	// go consumergroup(1, (topictemp + strconv.Itoa(0)))
	go prodconStarter(topictemp)

	// <- finished
	// go prodcon(2, messages, "test2", "test3", finished, finishedsending, finishedconsumtion)
	go consumer(1, messages, (topictemp + strconv.Itoa(countprodcon)), 1) //--> bei ID=3 gibt es Probleme ????
	<-finishedsending
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
	sessionStarttime = time.Now().UnixNano()
	jsonString := ""

	for i := 0; i < sendmessages; i++ {

		jsonMsg.TheTime = strconv.Itoa(int(time.Now().UnixNano()))

		// select compression / message format
		switch compressionType {
		case "":
			jsonOutput, _ := json.Marshal(&jsonMsg)
			jsonString = (string)(jsonOutput)
			// println(jsonString)

		case "avro":
			jsonOutput := encodeAvro(jsonMsg.TheTime, jsonMsg.ScareMe, jsonMsg.Binaryfile)
			jsonString = (string)(jsonOutput)
		default:
			jsonOutput, _ := json.Marshal(&jsonMsg)
			jsonString = (string)(jsonOutput)
			// println(jsonString)

		}
// send message
messageStartTime := time.Now()

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
	finishedsending <- true
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

	// // read the message
	// msg := <-consumer.Messages() // --> steht diese Funktion hier, wird anscheinend nur eine einzige Nachricht komsumiert !!!

	for i := 0; i < sendmessages; i++ {
		// fmt.Print(" ... waiting for message ...")
			// read the message
	msg := <-consumer.Messages() //--> muss diese funktion immer wieder aufgerufen werden, oder consumiert sie alle Nachrichten (wie ist es bei RabbitMQ???)

		messageReceivedTime := time.Now().UnixNano()	// --> wenn die Zeit hier genommen wird, wird die Laufzeit der Forschleife mit eingerechnet

		keyString := string(msg.Key)

		if keyString != "myInfo" {
			fmt.Println("received key is not myInfo ... sorry ... message ignore")
			continue
		}

		var jsonRecord MyInfo
		// check compressionType
		switch compressionType {
		case "":
			println(string(msg.Value))// --> falche Zeiten kommen hier bereits an !!!!!
			json.Unmarshal(msg.Value, &jsonRecord)
		case "avro":
			println(string(msg.Value)) // --> falche Zeiten kommen hier bereits an !!!!!
			jsonRecord = decodeAvro(msg.Value)
		}

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
// consumergroup test
type exampleConsumerGroupHandler struct{}

func (exampleConsumerGroupHandler) Setup(_ sarama.ConsumerGroupSession) error   { return nil }
func (exampleConsumerGroupHandler) Cleanup(_ sarama.ConsumerGroupSession) error { return nil }
func (h exampleConsumerGroupHandler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	// wird für jede partition in den angegebenen Topic asl seperate go routine gestartet !!!!!
	// fmt.Printf("claming partition %d .... \n", testing)
	// testing = testing + 1

		i:=0
		var jsonRecord MyInfo

	for msg := range claim.Messages() {
		if i < (messages){
		// fmt.Printf("Counter in partition go routine %d: %d \n", testing, i)
		fmt.Printf("Message topic:%q partition:%d offset:%d\n", msg.Topic, msg.Partition, msg.Offset)
		sess.MarkMessage(msg, "")

			messageReceivedTime := time.Now().UnixNano()	// --> wenn die Zeit hier genommen wird, wird die Laufzeit der Forschleife mit eingerechnet

		// 	keyString := string(msg.Key)

		// 	if keyString != "myInfo" {
		// 		fmt.Println("received key is not myInfo ... sorry ... message ignore")
		// 		continue
		// 	}

			json.Unmarshal(msg.Value, &jsonRecord)

			timevalue, err := strconv.ParseInt(jsonRecord.TheTime, 10, 64)
			if err != nil {
				log.Fatal("%s", err)
			}

			duration:= messageReceivedTime - timevalue
			durationMs := float64(duration) / float64(1000000) //Nanosekunden in Milisekunden

			consumeTime[i] = strconv.FormatFloat(durationMs, 'f', 6, 64)
			println(durationMs)
			completeTime = completeTime + durationMs
			println(completeTime) 

		// 	// fmt.Printf("got myInfo: %d \n", i)
		// 	// fmt.Print(jsonRecord)
		// 	// fmt.Println()

		// 	// messageEndTime:= time.Since(messageStartTime).Seconds()*1000
		// 	// consumeTime[i] = strconv.FormatFloat(messageEndTime, 'f', 6, 64)
		// 	// completeTime = completeTime + messageEndTime
		// }
		i = i+1
		}
		if i == messages {
			break
		}

	}
	// finishedgroup <- true
	return nil
}

func consumergroup(consumerID int, targetTopic1 string) {
	// <- finishedsending
	assignor := "roundrobin"
	// the topic where we want to listen at
	topic := targetTopic1

	fmt.Printf("Starting Consumergroup %d \n", consumerID)
	fmt.Printf("Consumergroup %d Topic to consume: %s \n",consumerID, targetTopic1)

// define group
	kfversion, err := sarama.ParseKafkaVersion("0.12.0.2") // kafkaVersion is the version of kafka server like 0.11.0.2
	if err != nil {
		log.Println(err)
	}
	
	config := sarama.NewConfig()
	config.Version = kfversion
	config.Consumer.Return.Errors = true
	config.Consumer.Offsets.Initial = sarama.OffsetOldest

	switch assignor {
	case "roundrobin":
		config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRoundRobin
	case "range":
		config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRange
	default:
		log.Panicf("Unrecognized consumer group partition assignor: %s", assignor)
	}
	
	// Start with a client
	client, err := sarama.NewClient(brokers, config)
	if err != nil {
		log.Println(err)
	}
	defer func() { _ = client.Close() }()

// Start a new consumer group
group, err := sarama.NewConsumerGroupFromClient("test-group6", client)
if err != nil {
    log.Println(err)
}
defer func() { _ = group.Close() }()

	// Track errors
	go func() {
		for err := range group.Errors() {
			fmt.Println("ERROR", err)
		}
	}()

// Iterate over consumer sessions.
ctx := context.Background()
sendmessages := messages
fmt.Printf("Start consumergroup: %d listening to some messages ... please send me something ... \n", consumerID)

starttime := time.Now()

// for {
		println("Message will be claimed")
		topics := []string{topic}
		handler := exampleConsumerGroupHandler{}
		err2 := group.Consume(ctx, topics, handler) //startet eine seperate go routine für jede Partition (über alle topics hinweg)
		if err2 != nil {
			log.Println(err)
		}

	// println("Message claimed but not printed !!!")
// }
	elapsed := time.Since(starttime)
	// compute complete time
	// correct the complete time --> complete time for sending and receiving != sum(sendtime + receivetime of all messages)
	sessionEndtime := time.Now().UnixNano()
	sessionEndtimeMS := float64(sessionEndtime) / float64(1000000) //Nanosekunden in Milisekunden
	sessionStarttimeMS := float64(sessionStarttime) / float64(1000000) //Nanosekunden in Milisekunden
	duration := sessionEndtimeMS - sessionStarttimeMS
	completeTime = duration

	fmt.Printf("Consumergroup: %d receives %d Messages -- elapsed time: %s \nAveragetime per message: %s \n", consumerID, sendmessages, elapsed, elapsed/time.Duration(sendmessages))
	finishedconsumtion <- true
	return
}

func prodcon(consendID int, messages int, targetTopic1 string, conPartition int32, targetTopic2 string, targetPartition int32, compressionType string) {
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
		// check compressionType
		switch compressionType {
		case "":
			json.Unmarshal(msg.Value, &jsonRecord)
		case "avro":
			jsonRecord = decodeAvro(msg.Value)
		}

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

		jsonString := ""

		// select compression / message format
		switch compressionType {
		case "":
			jsonOutput, _ := json.Marshal(&jsonRecord)
			jsonString = (string)(jsonOutput)	
		case "avro":
			jsonOutput := encodeAvro(jsonRecord.TheTime, jsonRecord.ScareMe, jsonRecord.Binaryfile)
			jsonString = (string)(jsonOutput)
		default:
			jsonOutput, _ := json.Marshal(&jsonRecord)
			jsonString = (string)(jsonOutput)	
		}

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
		go prodcon(i, messages, (topictemp + strconv.Itoa(i-1)), 1, (topictemp + strconv.Itoa(i)), 1, compressionType)
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

func encodeAvro(theTime string, scareMe string, binary []byte) []byte{
	// println("avro compression starting....")
    var b bytes.Buffer
    // foo := bufio.NewWriter(&b)

	codec, err := goavro.NewCodec(loginEventAvroSchema)
	if err != nil {
		panic(err)
	}

	var values []map[string] interface{}
	// values = append(values, m)
	// values = append(values, map[string] interface{}{"TheTime": "643573245", "ScareMe": "scareMe", "Binaryfile": []byte{0, 1, 2, 4, 5}})
	// values = append(values, map[string] interface{}{"TheTime": "657987654", "ScareMe": "scareMe", "Binaryfile": []byte{0, 1, 2, 4, 5}})
	values = append(values, map[string] interface{}{"TheTime": theTime, "ScareMe": scareMe, "Binaryfile": binary})

	// f, err := os.Create("event.avro")
	// if err != nil {
	// 	panic(err)
	// }

	// test json vor codierung
	// var jsonMsg MyInfo

	ocfw, err := goavro.NewOCFWriter(goavro.OCFConfig{
		W:     &b,
		Codec: codec,
	})
	if err != nil {
		panic(err)
	}
	// println(values)
	// println("Appending bytes")
	ocfw.Append(values)
	// if err = ocfw.Append(values); err != nil {
	// 	panic(err)
	// }

	bytearray:= b.Bytes()
	// control printout
	// println(b.Cap())

	// s := string(bytearray)
	// fmt.Println(s)


	// decode this shit to verify
	// var d bytes.Buffer
	// ocfr, err := goavro.NewOCFReader(&b)

	// var decoded interface{}
	// println(ocfr.Scan())

	// for ocfr.Scan(){
	// 		decoded, err = ocfr.Read()
	// 		if err != nil {
	// 			fmt.Fprintf(os.Stderr, "%s\n", err)
	// 		}
	// 		// s := string(decoded)
	// 		// fmt.Println(s)
	// 		fmt.Println(decoded)
	// 		// fmt.Printf("Test mit Printf: %v \n", decoded)
	// }

	// jsontest, err := json.Marshal(decoded)
	// if err != nil{
	// 	log.Fatal(err)
	// }
	// println(string(jsontest))
	// json.Unmarshal(jsontest, &jsonMsg)
	// // json.Unmarshal(jsontest, &jsonMsg2)

	// // fmt.Printf("Myinfo1: %s \n", jsonMsg)
	// fmt.Printf("Time: %s \n", jsonMsg.TheTime)
	// fmt.Printf("ScareMe: %s \n", jsonMsg.ScareMe)
	// fmt.Printf("Binary: %s \n", jsonMsg.Binaryfile)

	// println("avro compression finished!!!")

	return bytearray
}

func decodeAvro(message []byte) MyInfo{
	var output MyInfo
	b := bytes.NewBuffer(message)
	
	println(message)
	// decode this shit to verify
	ocfr, err := goavro.NewOCFReader(b)

	var decoded interface{}
	// println(ocfr.Scan()) //--> if true, messages available

	for ocfr.Scan(){
			decoded, err = ocfr.Read()
			if err != nil {
				fmt.Fprintf(os.Stderr, "%s\n", err)
			}
			// fmt.Println(decoded)
			// fmt.Printf("Test mit Printf: %v \n", decoded)
	}

	jsontest, err := json.Marshal(decoded)
	if err != nil{
		log.Fatal(err)
	}
	// println(string(jsontest))
	json.Unmarshal(jsontest, &output)

	// fmt.Printf("Myinfo1: %s \n", output)
	fmt.Printf("Time: %s \n", output.TheTime)
	// fmt.Printf("ScareMe: %s \n", output.ScareMe)
	// fmt.Printf("Binary: %s \n", output.Binaryfile)

	return output
}