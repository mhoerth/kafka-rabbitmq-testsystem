package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"strconv"
	"time"
	"log"
	"os/exec"

	"mom-test/encoding"
	"mom-test/structs"
	"mom-test/output"

	"github.com/Shopify/sarama"
)

var finishedconsumtion = make(chan bool, 10)
var finishedsending = make(chan bool, 10)
var finishedgroup = make(chan bool, 10)
var brokers []string
var countprodcon int

var messages int
var messageSize string
var sessionStarttime int64

var testing int

var compressionType string

// for csv file writing
var csvStruct structs.Csv

// Kafka starts a Kafka Producer and consumer with the option to add up to 6 instances which are consuming and producing (changing the message a bit)
// , to define an encoding format, and the size of the binary message included in the message sent to the message bus system, the topicname to send to and the message amount
func Kafka(interations int, messageamount int, topic string, conProdInst int, compression string, sizeOfMessaage string, delayTime int, synchronicity string ) {

	topictemp := topic
	messages = messageamount
	countprodcon = conProdInst
	compressionType = compression
	messageSize = sizeOfMessaage

	if synchronicity == "sync"{
		csvStruct.Testsystem = "SyncKafka"
	}
	if synchronicity == "async"{
		csvStruct.Testsystem = "AsyncKafka"
	}
	csvStruct.Messages = messageamount
	csvStruct.CountProdCon = conProdInst
	csvStruct.MessageSize = sizeOfMessaage
	csvStruct.CompressionType = compression
	csvStruct.MsDelay = delayTime
	
	brokers = []string{"127.0.0.1:9092"}

	for i:=0; i<interations; i++{

		cpuStart, err := exec.Command("bash", "-c", "cat /proc/stat |grep cpu").Output()
		if err != nil {
			panic(err)
		}	

		csvStruct.Interation = i
		configEnv(topictemp)
		starttime := time.Now()
		if synchronicity == "sync"{
			go syncProducer(0, messages, (topictemp + strconv.Itoa(0)), 0)
		}
		if synchronicity == "async"{
			go asyncProducer(1, messages, (topictemp + strconv.Itoa(0)), 0)
		}
		// go consumergroup(1, (topictemp + strconv.Itoa(0)))
		go prodconStarter(topictemp)
	
		go consumer(csvStruct.CountProdCon, messages, (topictemp + strconv.Itoa(countprodcon)), 0)
		<-finishedsending
		<-finishedconsumtion
	
		elapsed := time.Since(starttime)
		fmt.Printf("Elapsed time for sending and consuming: %s \nAveragetime per message: %s \n", elapsed, elapsed/time.Duration(messages))
			
		cpuStop, err := exec.Command("bash", "-c", "cat /proc/stat |grep cpu").Output()
		if err != nil {
			panic(err)
		}	
		println("Writing CSV file")
		// go output.Csv("Kafka", messages, sendTime, consumeTime, encodingTime, countprodcon, consendTime, decodingTime, completeTime, messageSize, compressionType)
		// compute RoundTripTime
		println("Compute RTT and ConsumeTime")
		csvStruct.RoundTripTime = output.ComputeRoundTripTime(csvStruct.SendTimeStamps, csvStruct.ConsumeTimeStamps, csvStruct.Messages)
		csvStruct.ConsumeTime = output.ComputeConsumeTime(csvStruct.EncodingTime, csvStruct.SendTime, csvStruct.ConsumeTime, csvStruct.CountProdCon, csvStruct.Messages)
		// write csv file
		output.Csv(csvStruct, cpuStart, cpuStop) //csvStruct is too large for a go routine
		deleteConfigEnv(topictemp)
		time.Sleep(100000000) //wait 1 second before next testexecution
	}
}
// CloseChannels is used channels, needed for the feature to do multiple interations with he same config provided with one call out of the main function
func CloseChannels(){
	close(finishedsending)
	close(finishedconsumtion)
}

// cofigure the kafka test environment
// define topics, partitions and replicationfactor
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
	// topics, _ := cluster.Topics()
	partitiontemp:= topictemplate
		for i:=0; i<=countprodcon; i++{
		// 	if contains(topics, (partitiontemp + strconv.Itoa(i))) == false{
		// 		err = admin.CreateTopic((partitiontemp + strconv.Itoa(i)), &sarama.TopicDetail{
		// 			NumPartitions:     1,
		// 			ReplicationFactor: 1,
		// 		}, false)
		// 		if err != nil {
		// 			panic(err)
		// 		}else{
		// 			fmt.Printf("Topic %s created!!! \n", (partitiontemp + strconv.Itoa(i)))
		// 		}
		// 	}
		// }


		for {
			created := false
			//get all topic from cluster
			topicList, err := admin.ListTopics()
			for value := range topicList{
				s := fmt.Sprintf("%s_", value)
				fmt.Println(s)
				if value == (partitiontemp + strconv.Itoa(i)){					
					created = true
					break
				}else{
					err = admin.CreateTopic((partitiontemp + strconv.Itoa(i)), &sarama.TopicDetail{
						NumPartitions:     1,
						ReplicationFactor: 1,
					}, false)
					if err != nil {
						log.Println(err)
					}
					fmt.Println("Wait a second")
					time.Sleep(1000000000); //wait 1 second before next testexecution
				}
			}
			if created == true{
				break
			}
		}
		fmt.Printf("Topic %s created!!! \n", (partitiontemp + strconv.Itoa(i)))
	}
}

// cleanup the testsystem configuration to avoid measure mistakes after one test execution
// without this function messages are persistent stored in the message bus and the consumer can imediatly consume the required messages !!!
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
				// wait until topic is deleted !!!
				for {
					deleted := false
					//get all topic from cluster
					topicList, err := admin.ListTopics()
					for value := range topicList{
						s := fmt.Sprintf("%s_", value)
						fmt.Println(s)
						if value == (partitiontemp + strconv.Itoa(i)){
							err = admin.DeleteTopic((partitiontemp + strconv.Itoa(i)))
							if err != nil {
								panic(err)
							}
							fmt.Println("Wait a second")
							deleted = true
							time.Sleep(1000000000); //wait 1 second before next testexecution
						}
					}
					if deleted == false{
						break
					}
				}	
				fmt.Printf("Topic %s deleted!!! \n", (partitiontemp + strconv.Itoa(i)))
			}
	}

// starts a syncProducer instnce for kafka 
func syncProducer(producerid int, messages int, targetTopic1 string, targetPartition int32) {

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

	// create a sync producer
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
	var jsonMsg structs.MyInfo
	var startTime int64
	var jsonOutput []byte

	if testifleinput == nil {
		// testifleinput, err = ioutil.ReadFile("../../output-" + messageSize + "Kibi-rand")
		if messageSize != ""{
			testifleinput, err = ioutil.ReadFile(messageSize)
			if err != nil {
				fmt.Println(err)
			}
		}
	}

	jsonMsg.ScareMe = "Yes, please"
	jsonMsg.Binaryfile = testifleinput
	// sessionStarttime = time.Now().UnixNano()
	jsonString := ""

	for i := 0; i < sendmessages; i++ {

		// wait for each send interval --> create a fixed transfer rate of messages
		if i != 0{
			// if (i % 100) == 0{
				// println("Wait 100 msec before sending again 100 messages")
				time.Sleep(time.Duration(csvStruct.MsDelay) * time.Millisecond)
			// }
		}
		// set sessionStartTime (Time the first message (i==0) was send)
		if i == 0{
			sessionStarttime = time.Now().UnixNano()
		}
		
		// select compression / message format
		switch compressionType {
		case "json":
			jsonMsg.TheTime = strconv.Itoa(int(time.Now().UnixNano()))	//--> important to use this command twice, because of accourate time measurement !
			startTime = time.Now().UnixNano()
			jsonOutput, _ := json.Marshal(&jsonMsg)

			jsonString = (string)(jsonOutput)
			// println(jsonString)
		case "avro":
			jsonOutput, startTime = encoding.EncodeAvro(0, i, jsonMsg.ScareMe, jsonMsg.Binaryfile) //get encoded message + encoding time
			jsonString = (string)(jsonOutput)
		case "proto":
			jsonOutput, startTime = encoding.EncodeProto(0, i, jsonMsg.ScareMe, jsonMsg.Binaryfile)
			jsonString = (string)(jsonOutput)
		}

		msg := &sarama.ProducerMessage{
			Topic: topic,
			Key:   sarama.StringEncoder("myInfo"),
			Value: sarama.StringEncoder(jsonString),
			Partition: partition,
		}

		// fmt.Println("Sending Message : ")
		// fmt.Println(msg)

		// send message
		messageStartTime := time.Now()

		// write encoding time
		duration:= messageStartTime.UnixNano() - startTime
		durationMs := float64(duration) / float64(1000000) //Nanosekunden in Milisekunden
		csvStruct.EncodingTime[producerid][i] = strconv.FormatFloat(durationMs, 'f', 6, 64)

			// sync producer
			_, _, err := producer.SendMessage(msg)

			if err != nil {
				println(len(jsonString))	//which size has the complete message? Why is the added information to larger messages so different to small ones?
				panic(err)
			}

		messageEndTime:= time.Since(messageStartTime).Seconds()*1000

		// // set start of the round trip time
		// csvStruct.RoundTripTime [i] = strconv.FormatFloat(float64(messageStartTime.UnixNano()), 'f', 6, 64)
		csvStruct.SendTimeStamps[i] = strconv.FormatInt(messageStartTime.UnixNano(), 10)

		if i < 3{
			fmt.Printf("Size of msg: %d \n", len(jsonString))
			csvStruct.Filesize = int64(len(jsonString))
		}
		csvStruct.SendTime[producerid][i] = strconv.FormatFloat(messageEndTime, 'f', 6, 64)
		// completeTime = completeTime + messageEndTime
		// fmt.Printf("Message %d send to partition %d offset %d \n", i, partition, offset)

	}
	elapsed := time.Since(starttime)
	fmt.Printf("Producer: %d send %d Messages -- elapsed time: %s \nAveragetime per message: %s \n", producerid, sendmessages, elapsed, elapsed/time.Duration(sendmessages))
	finishedsending <- true
	return
}

// starts a asyncProducer instnce for kafka 
func asyncProducer(producerid int, messages int, targetTopic1 string, targetPartition int32) {

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

	// for async
	config.Producer.Return.Successes = false
	//for batching ???
	config.Producer.Flush.MaxMessages = 1
	config.Producer.Flush.Messages = 1

	config.Producer.Partitioner = sarama.NewManualPartitioner //set the partition amnually siplyifes the test (no complex get and set partition!!!)

	// brokers := []string{"127.0.0.1:9092"}

	// create a async producer
		producer, err := sarama.NewAsyncProducer(brokers, config)

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
	var jsonMsg structs.MyInfo
	var startTime int64
	var jsonOutput []byte

	if testifleinput == nil {
		// testifleinput, err = ioutil.ReadFile("../../output-" + messageSize + "Kibi-rand")
		if messageSize != ""{
			testifleinput, err = ioutil.ReadFile(messageSize)
			if err != nil {
				fmt.Println(err)
			}
		}
	}

	jsonMsg.ScareMe = "Yes, please"
	jsonMsg.Binaryfile = testifleinput
	// sessionStarttime = time.Now().UnixNano()
	jsonString := ""

	for i:=0; i < sendmessages; i++ {

		// wait for each send interval --> create a fixed transfer rate of messages
		if i != 0{
			// if (i % 100) == 0{
				// println("Wait 100 msec before sending again 100 messages")
				time.Sleep(time.Duration(csvStruct.MsDelay) * time.Millisecond)
			// }
		}
		// set sessionStartTime (Time the first message (i==0) was send)
		if i == 0{
			sessionStarttime = time.Now().UnixNano()
			// println(sessionStarttime)
		}
		
		// select compression / message format
		switch compressionType {
		case "json":
			jsonMsg.TheTime = strconv.Itoa(int(time.Now().UnixNano()))	//--> important to use this command twice, because of accourate time measurement !
			startTime = time.Now().UnixNano()
			jsonOutput, _ := json.Marshal(&jsonMsg)

			jsonString = (string)(jsonOutput)
			// println(jsonString)
		case "avro":
			jsonOutput, startTime = encoding.EncodeAvro(0, i, jsonMsg.ScareMe, jsonMsg.Binaryfile) //get encoded message + encoding time
			jsonString = (string)(jsonOutput)
		case "proto":
			jsonOutput, startTime = encoding.EncodeProto(0, i, jsonMsg.ScareMe, jsonMsg.Binaryfile)
			jsonString = (string)(jsonOutput)
		}

		msg := &sarama.ProducerMessage{
			Topic: topic,
			Key:   sarama.StringEncoder("myInfo"),
			Value: sarama.StringEncoder(jsonString),
			Partition: partition,
		}

		// fmt.Println("Sending Message : ")
		// fmt.Println(msg)

		// send message
		messageStartTime := time.Now()

		// write encoding time
		duration:= messageStartTime.UnixNano() - startTime
		durationMs := float64(duration) / float64(1000000) //Nanosekunden in Milisekunden
		csvStruct.EncodingTime[0][i] = strconv.FormatFloat(durationMs, 'f', 6, 64)

			// async producer
			// fmt.Println(i)
			// producer.Input() <- msg
			// println("after sending")

			// messageSend := false
			// println("before sneding")
			producer.Input() <- msg
			// println("Producer: " + strconv.Itoa(i))
			// for{
				// select {
				// // case producer.Input() <- msg:
				// // 	// messageSend = true
				// // 	// <- producer.Successes()
				// // 	success := <- producer.Successes()
				// // 	println("Send message:" + strconv.Itoa(i))

				// // 	if i == 0{
				// // 		fmt.Printf("Sent message value='%s' at partition = %d at topic %s, offset = %d\n", success.Value, success.Partition, success.Topic, success.Offset)
				// // 	}

				// case err := <- producer.Errors():
				// 	fmt.Println("Failed to produce message", err)
				// // case success := <- producer.Successes():
				// // 	// if i == 1{
				// // 		fmt.Printf("Sent message value='%s' at partition = %d at topic %s, offset = %d\n", success.Value, success.Partition, success.Topic, success.Offset)
				// // 	// }
					if i < 3{
						fmt.Printf("Size of msg: %d \n", len(jsonString))
						csvStruct.Filesize = int64(len(jsonString))
					}
				// 	// messageSend = true

				// default:
				// 	// wait for each send interval --> create a fixed transfer rate of messages
				// 	// time.Sleep(time.Duration(csvStruct.MsDelay) * time.Millisecond)
				// }	

				// if messageSend == true{
				// 	break
				// }
			// }

		messageEndTime:= time.Since(messageStartTime).Seconds()*1000

		// // set start of the round trip time
		// csvStruct.RoundTripTime [i] = strconv.FormatFloat(float64(messageStartTime.UnixNano()), 'f', 6, 64)
		csvStruct.SendTimeStamps[i] = strconv.FormatInt(messageStartTime.UnixNano(), 10)
		// fmt.Println(csvStruct.SendTimeStamps[i])

		csvStruct.SendTime[0][i] = strconv.FormatFloat(messageEndTime, 'f', 6, 64)
		// completeTime = completeTime + messageEndTime
		// fmt.Printf("Message %d send to partition %d offset %d \n", i, partition, offset)

		if i == sendmessages{
			break
		}

	}
	elapsed := time.Since(starttime)
	fmt.Printf("Producer: %d send %d Messages -- elapsed time: %s \nAveragetime per message: %s \n", producerid, sendmessages, elapsed, elapsed/time.Duration(sendmessages))
	finishedsending <- true
	return
}

// starts a consumer instance for kafka
func consumer(consumerID int, messages int, targetTopic1 string, targetPartition int32) {

	fmt.Printf("Starting Consumer %d \n", consumerID)
	fmt.Printf("Consumer %d Topic to consume: %s \n",consumerID, targetTopic1)
	fmt.Printf("Consumer %d get partitionID: %d \n", consumerID, targetPartition)
	// we create a configuration structure for our kafka sarama api
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true

	// set consumtion size / messages
	config.Consumer.Fetch.Max = int32(csvStruct.Filesize)

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
		// println(string(msg.Value))// --> falche Zeiten kommen hier bereits an !!!!!

		// // set start of the round trip time
		// beginTime, err := strconv.ParseFloat(csvStruct.RoundTripTime [i], 64)
		// if err != nil{
		// 	panic(err)
		// }
		// csvStruct.RoundTripTime [i] = strconv.FormatFloat((float64(messageReceivedTime) - beginTime), 'f', 6, 64)
		csvStruct.ConsumeTimeStamps[i] = strconv.FormatInt(messageReceivedTime, 10)

		keyString := string(msg.Key)

		if keyString != "myInfo" {
			fmt.Println("received key is not myInfo ... sorry ... message ignore")
			continue
		}

		var jsonRecord structs.MyInfo
		var endTime int64
		// check compressionType
		switch compressionType {
		case "json":
			// println(string(msg.Value))// --> falche Zeiten kommen hier bereits an !!!!!
			// startTime := time.Now().UnixNano()
			json.Unmarshal(msg.Value, &jsonRecord)
			endTime = time.Now().UnixNano()

		case "avro":
			// println(string(msg.Value)) // --> falche Zeiten kommen hier bereits an !!!!!
			jsonRecord, endTime = encoding.DecodeAvro(0, i, msg.Value)
		case "proto":
			// println(string(msg.Value)) // --> falche Zeiten kommen hier bereits an !!!!!
			jsonRecord, endTime = encoding.DecodeProto(0, i, msg.Value)
		}

		// write decodingTime
		duration:= endTime - messageReceivedTime
		durationMs := float64(duration) / float64(1000000) //Nanosekunden in Milisekunden
		csvStruct.DecodingTime[consumerID][i] = strconv.FormatFloat(durationMs, 'f', 6, 64)

		// println(jsonRecord.TheTime)
		timevalue, err := strconv.ParseInt(jsonRecord.TheTime, 10, 64)
		if err != nil {
			log.Fatal("%s", err)
		}
		// // set sessionStartTime (Time the first message (i==0) was send)
		// if i == 0{
		// 	sessionStarttime = timevalue
		// }

		duration = messageReceivedTime - timevalue
		// fmt.Println("Timestamp: %d ReceivedTime: %d Result: %d", timevalue, messageReceivedTime, duration)

		durationMs = float64(duration) / float64(1000000) //Nanosekunden in Milisekunden

		// // get values fo encodingTime and sendTime
		// encodingTime, err := strconv.ParseFloat(csvStruct.EncodingTime[csvStruct.CountProdCon][i], 64)
		// if err != nil{
		// 	panic(err)
		// }

		// sendTime, err := strconv.ParseFloat(csvStruct.SendTime[csvStruct.CountProdCon][i], 64)
		// if err != nil{
		// 	panic(err)
		// }

		// // measured consumeTime includes encoding- and sendTime, therefore we have to substract
		// durationMs = durationMs - (encodingTime + sendTime)
		csvStruct.ConsumeTime[consumerID][i] = strconv.FormatFloat(durationMs, 'f', 6, 64)

		// completeTime = completeTime + durationMs	 
		// compute complete time
		// correct the complete time --> complete time for sending and receiving != sum(sendtime + receivetime of all messages)
		if i == (sendmessages -1){
			sessionEndtime := time.Now().UnixNano()
			// println(sessionEndtime)
			// println(sessionStarttime)
			sendReceiveDuration := sessionEndtime - sessionStarttime
			// sessionEndtimeMS := float64(sessionEndtime) / float64(1000000) //Nanosekunden in Milisekunden
			// sessionStarttimeMS := float64(sessionStarttime) / float64(1000000) //Nanosekunden in Milisekunden
			sendReceiveDurationMs := float64(sendReceiveDuration) / float64(1000000) //Nanosekunden in Milisekunden
			csvStruct.CompleteTime = sendReceiveDurationMs	
		}


		// fmt.Printf("got myInfo: %d \n", i)
		// fmt.Print(jsonRecord)
		// fmt.Println()
	}
	elapsed := time.Since(starttime)
	fmt.Printf("Consumer: %d receives %d Messages -- elapsed time: %s \nAveragetime per message: %s \n", consumerID, sendmessages, elapsed, elapsed/time.Duration(sendmessages))
	finishedconsumtion <- true
	return
}
// consumergroup test (example prototype)
type exampleConsumerGroupHandler struct{}

func (exampleConsumerGroupHandler) Setup(_ sarama.ConsumerGroupSession) error   { return nil }
func (exampleConsumerGroupHandler) Cleanup(_ sarama.ConsumerGroupSession) error { return nil }
func (h exampleConsumerGroupHandler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	// wird für jede partition in den angegebenen Topic asl seperate go routine gestartet !!!!!
	// fmt.Printf("claming partition %d .... \n", testing)
	// testing = testing + 1

		i:=0
		var jsonRecord structs.MyInfo

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

			csvStruct.ConsumeTime[0][i] = strconv.FormatFloat(durationMs, 'f', 6, 64)
			println(durationMs)
			// completeTime = completeTime + durationMs
			// println(completeTime) 

		// 	// fmt.Printf("got myInfo: %d \n", i)
		// 	// fmt.Print(jsonRecord)
		// 	// fmt.Println()
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

// example prototype of a working consumergroup
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
	csvStruct.CompleteTime = duration

	fmt.Printf("Consumergroup: %d receives %d Messages -- elapsed time: %s \nAveragetime per message: %s \n", consumerID, sendmessages, elapsed, elapsed/time.Duration(sendmessages))
	finishedconsumtion <- true
	return
}

// starting a process with a consumer and a AsyncProducer to simulate multiple message exchanges over the message bus
func asyncProdcon(consendID int, messages int, targetTopic1 string, conPartition int32, targetTopic2 string, targetPartition int32, compressionType string) {
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
	configProducer.Producer.Return.Successes = false
	//for batching ???
	configProducer.Producer.Flush.MaxMessages = 1
	configProducer.Producer.Flush.Messages = 1
	//config consumer
	configConsumer.Consumer.Return.Errors = true
	// set consumtion size / messages
	configConsumer.Consumer.Fetch.Max = int32(csvStruct.Filesize)

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

	producerInst, err := sarama.NewAsyncProducer(brokers, configProducer)
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

	for i := 0; i < sendmessages; i++ {
		// fmt.Print(" ... waiting for message ...")
			
		// read the message
		msg := <-consumerinst.Messages()

		messageReceivedTime := time.Now().UnixNano()	// --> wenn die Zeit hier genommen wird, wird die Laufzeit der Forschleife mit eingerechnet

		keyString := string(msg.Key)
		// println(string(msg.Value))

		if keyString != "myInfo" {
			fmt.Println("received key is not myInfo ... sorry ... message ignore")
			continue
		}

		var jsonRecord structs.MyInfo
		var endTime int64
		var startTime int64
		var jsonOutput []byte
		// check compressionType
		switch compressionType {
		case "json":
			// startTime := time.Now().UnixNano()
			json.Unmarshal(msg.Value, &jsonRecord)
			endTime = time.Now().UnixNano()

		case "avro":
			jsonRecord, endTime = encoding.DecodeAvro(consendID, i, msg.Value)
		case "proto":
			jsonRecord, endTime = encoding.DecodeProto(consendID, i, msg.Value)
		}

		// write decodingTime
		duration:= endTime - messageReceivedTime
		durationMs := float64(duration) / float64(1000000) //Nanosekunden in Milisekunden
		csvStruct.DecodingTime[consendID][i] = strconv.FormatFloat(durationMs, 'f', 6, 64)

		timevalue, err := strconv.ParseInt(jsonRecord.TheTime, 10, 64)
		if err != nil {
			log.Fatal("%s", err)
		}

		duration = messageReceivedTime - timevalue

		// // get values fo encodingTime and sendTime
		// encodingTime, err := strconv.ParseFloat(csvStruct.EncodingTime[consendID -1][i], 64)
		// if err != nil{
		// 	panic(err)
		// }

		// sendTime, err := strconv.ParseFloat(csvStruct.SendTime[consendID -1][i], 64)
		// if err != nil{
		// 	panic(err)
		// }

		durationMs  = float64(duration) / float64(1000000) //Nanosekunden in Milisekunden

		// // measured consumeTime includes encoding- and sendTime, therefore we have to substract
		// durationMs = durationMs - (encodingTime + sendTime)

		// println(durationMs)
		csvStruct.ConsumeTime[consendID -1][i] = strconv.FormatFloat(durationMs, 'f', 6, 64)		//consendID -1 because of the relation producer number 1 get ID 0 and consumer 1 max(CountProdCon)
		// completeTime = completeTime + durationMs	 


		// fmt.Printf("got myInfo number %d on consumer + producer %d \n", i, consumerID)

		jsonRecord.ScareMe = jsonRecord.ScareMe + strconv.Itoa(consendID)

		jsonString := ""

		// wait for each send interval --> create a fixed transfer rate of messages
		// if i != 0{
		// 	// if (i % 100) == 0{
		// 		// println("Wait 100 msec before sending again 100 messages")
		// 		time.Sleep(time.Duration(csvStruct.MsDelay) * time.Millisecond)
		// 	// }
		// }	

		// select compression / message format
		switch compressionType {
		case "json":
			jsonRecord.TheTime = strconv.Itoa(int(time.Now().UnixNano()))
			startTime = time.Now().UnixNano()
			jsonOutput, _ := json.Marshal(&jsonRecord)
			// endTime := time.Now().UnixNano()

			jsonString = (string)(jsonOutput)	
		case "avro":
			jsonOutput, startTime = encoding.EncodeAvro(consendID, i, jsonRecord.ScareMe, jsonRecord.Binaryfile)
			jsonString = (string)(jsonOutput)
		case "proto":
			jsonOutput, startTime = encoding.EncodeProto(consendID, i, jsonRecord.ScareMe, jsonRecord.Binaryfile)
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

		// write encoding time
		duration = messageStartTime.UnixNano() - startTime
		durationMs  = float64(duration) / float64(1000000) //Nanosekunden in Milisekunden
		csvStruct.EncodingTime[consendID][i] = strconv.FormatFloat(durationMs, 'f', 6, 64)

			// async producer
			// messageSend := false
			producerInst.Input() <- msgout
			// println("Consumer+Producer: " + strconv.Itoa(i))

			// for{
				// select {
				// // case producerInst.Input() <- msgout:
				// // 	// messageSend = true
				// // 	// <- producerInst.Successes()
				// // 	success := <- producerInst.Successes()
				// // 	// println("Send message:" + strconv.Itoa(i))
				// // 	if i == 0{
				// // 		fmt.Printf("Sent message value='%s' at partition = %d at topic %s, offset = %d\n", success.Value, success.Partition, success.Topic, success.Offset)
				// // 	}

				// case err := <- producerInst.Errors():
				// 	fmt.Println("Failed to produce message", err)
				// // case success := <- producerInst.Successes():
				// // 	if i == 1{
				// // 		fmt.Printf("Sent message value='%s' at partition = %d at topic %s, offset = %d\n", success.Value, success.Partition, success.Topic, success.Offset)
				// // 	}
				// // 	if i < 3{
				// // 		fmt.Printf("Size of msg: %d \n", len(jsonString))
				// // 		csvStruct.Filesize = int64(len(jsonString))
				// // 	}
				// // 	if i < 1{
				// // 		fmt.Printf("Consumer + Producer %d sets partitionID: ", consendID)
				// // 		println(partition)
				// // 		fmt.Printf("Consumer + Producer %d Topic to send: %s \n", consendID, targetTopic2)
				// // 	}

				// default:
				// 	// wait for each send interval --> create a fixed transfer rate of messages
				// 	// time.Sleep(time.Duration(csvStruct.MsDelay) * time.Millisecond)
				// }	

			// 	if messageSend == true{
			// 		break
			// 	}
			// }

		messageEndTime:= time.Since(messageStartTime).Seconds()*1000
		csvStruct.SendTime[consendID][i] = strconv.FormatFloat(messageEndTime, 'f', 6, 64)
		// completeTime = completeTime + messageEndTime

		// fmt.Printf("Consumer + Producer %d send modified myInfo: %d to topic: %s \n", consumerID, i, targetTopic2)
		// fmt.Print(jsonRecord.ScareMe)
	}
	elapsed := time.Since(starttime)
	fmt.Printf("Consumer + Producer: %d receives and sends %d Messages -- elapsed time: %s \nAveragetime per message: %s \n", consendID, sendmessages, elapsed, elapsed/time.Duration(sendmessages))
}

// starting a process with a consumer and a SyncProducer to simulate multiple message exchanges over the message bus
func syncProdcon(consendID int, messages int, targetTopic1 string, conPartition int32, targetTopic2 string, targetPartition int32, compressionType string) {
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
	// set consumtion size / messages
	configConsumer.Consumer.Fetch.Max = int32(csvStruct.Filesize)

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

	for i := 0; i < sendmessages; i++ {
		// fmt.Print(" ... waiting for message ...")
			
		// read the message
		msg := <-consumerinst.Messages()

		messageReceivedTime := time.Now().UnixNano()	// --> wenn die Zeit hier genommen wird, wird die Laufzeit der Forschleife mit eingerechnet

		keyString := string(msg.Key)
		// println(string(msg.Value))

		if keyString != "myInfo" {
			fmt.Println("received key is not myInfo ... sorry ... message ignore")
			continue
		}

		var jsonRecord structs.MyInfo
		var endTime int64
		var startTime int64
		var jsonOutput []byte
		// check compressionType
		switch compressionType {
		case "json":
			// startTime := time.Now().UnixNano()
			json.Unmarshal(msg.Value, &jsonRecord)
			endTime = time.Now().UnixNano()

		case "avro":
			jsonRecord, endTime = encoding.DecodeAvro(consendID, i, msg.Value)
		case "proto":
			jsonRecord, endTime = encoding.DecodeProto(consendID, i, msg.Value)
		}

		// write decodingTime
		duration:= endTime - messageReceivedTime
		durationMs := float64(duration) / float64(1000000) //Nanosekunden in Milisekunden
		csvStruct.DecodingTime[consendID][i] = strconv.FormatFloat(durationMs, 'f', 6, 64)

		timevalue, err := strconv.ParseInt(jsonRecord.TheTime, 10, 64)
		if err != nil {
			log.Fatal("%s", err)
		}

		duration = messageReceivedTime - timevalue

		// // get values fo encodingTime and sendTime
		// encodingTime, err := strconv.ParseFloat(csvStruct.EncodingTime[consendID -1][i], 64)
		// if err != nil{
		// 	panic(err)
		// }

		// sendTime, err := strconv.ParseFloat(csvStruct.SendTime[consendID -1][i], 64)
		// if err != nil{
		// 	panic(err)
		// }

		durationMs  = float64(duration) / float64(1000000) //Nanosekunden in Milisekunden

		// // measured consumeTime includes encoding- and sendTime, therefore we have to substract
		// durationMs = durationMs - (encodingTime + sendTime)

		// println(durationMs)
		csvStruct.ConsumeTime[consendID -1][i] = strconv.FormatFloat(durationMs, 'f', 6, 64)			//consendID -1 because of the relation producer number 1 get ID 0 and consumer 1 max(CountProdCon)
		// completeTime = completeTime + durationMs	 


		// fmt.Printf("got myInfo number %d on consumer + producer %d \n", i, consumerID)

		jsonRecord.ScareMe = jsonRecord.ScareMe + strconv.Itoa(consendID)

		jsonString := ""

		// wait for each send interval --> create a fixed transfer rate of messages
		// if i != 0{
		// 	// if (i % 100) == 0{
		// 		// println("Wait 100 msec before sending again 100 messages")
		// 		time.Sleep(time.Duration(csvStruct.MsDelay) * time.Millisecond)
		// 	// }
		// }	

		// select compression / message format
		switch compressionType {
		case "json":
			jsonRecord.TheTime = strconv.Itoa(int(time.Now().UnixNano()))
			startTime = time.Now().UnixNano()
			jsonOutput, _ := json.Marshal(&jsonRecord)
			// endTime := time.Now().UnixNano()

			jsonString = (string)(jsonOutput)	
		case "avro":
			jsonOutput, startTime = encoding.EncodeAvro(consendID, i, jsonRecord.ScareMe, jsonRecord.Binaryfile)
			jsonString = (string)(jsonOutput)
		case "proto":
			jsonOutput, startTime = encoding.EncodeProto(consendID, i, jsonRecord.ScareMe, jsonRecord.Binaryfile)
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

		// write encoding time
		duration = messageStartTime.UnixNano() - startTime
		durationMs  = float64(duration) / float64(1000000) //Nanosekunden in Milisekunden
		csvStruct.EncodingTime[consendID][i] = strconv.FormatFloat(durationMs, 'f', 6, 64)

		partition, _, err := producerInst.SendMessage(msgout)

		if err != nil {
			panic(err)
		}

		messageEndTime:= time.Since(messageStartTime).Seconds()*1000
		csvStruct.SendTime[consendID][i] = strconv.FormatFloat(messageEndTime, 'f', 6, 64)
		// completeTime = completeTime + messageEndTime

		if i < 1{
			fmt.Printf("Consumer + Producer %d sets partitionID: ", consendID)
			println(partition)
			fmt.Printf("Consumer + Producer %d Topic to send: %s \n", consendID, targetTopic2)
		// }
		}

		// fmt.Printf("Consumer + Producer %d send modified myInfo: %d to topic: %s \n", consumerID, i, targetTopic2)
		// fmt.Print(jsonRecord.ScareMe)
	}
	elapsed := time.Since(starttime)
	fmt.Printf("Consumer + Producer: %d receives and sends %d Messages -- elapsed time: %s \nAveragetime per message: %s \n", consendID, sendmessages, elapsed, elapsed/time.Duration(sendmessages))
}

// stating several instances of the consumer/producer process automatically consigured by the given class parameters (call of the main function)
func prodconStarter(topictemplate string){
	topictemp := topictemplate
	for i:=1; i <= countprodcon; i++{
		fmt.Printf("ProdCon %d in starting process \n", i)

		if csvStruct.Testsystem == "SyncKafka"{
			go syncProdcon(i, messages, (topictemp + strconv.Itoa(i-1)), 0, (topictemp + strconv.Itoa(i)), 0, compressionType)
		}
		if csvStruct.Testsystem == "AsyncKafka"{
			go asyncProdcon(i, messages, (topictemp + strconv.Itoa(i-1)), 0, (topictemp + strconv.Itoa(i)), 0, compressionType)
		}
	}
}

// helper function to get to know, whether a topic is still existent
func contains(array []string, search string) bool{
	for index := range array {
		if array[index] == search{
			return true
		}
	}
	return false
}