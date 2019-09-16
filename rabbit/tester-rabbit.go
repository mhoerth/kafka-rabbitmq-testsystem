package rabbit

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"strconv"
	"time"

	"../encoding"
	"../structs"
	"../output"

	"github.com/streadway/amqp"
)

var finishedconsumtion = make(chan bool, 10)
var finishedsending = make(chan bool, 10)
var messages int
var messageSize string
var countprodcon int

var sessionStarttime int64

var compressionType string
var csvStruct structs.Csv

// Rabbit starts a RabbitMQ producer and consumer with the option to add up to 6 instances which are consuming and producing (changing the message a bit)
// , to define an encoding format, and the size of the binary message included in the message sent to the message bus system, the queuename to send to and the message amount
func Rabbit(messageamount int, queue string, conProdInst int, compression string, sizeOfMessaage string) {

	queuetemp := queue
	messages = messageamount
	countprodcon = conProdInst
	compressionType = compression
	messageSize = sizeOfMessaage

	csvStruct.Testsystem = "Rabbit"
	csvStruct.Messages = messageamount
	csvStruct.CountProdCon = conProdInst
	csvStruct.MessageSize = sizeOfMessaage
	csvStruct.CompressionType = compression

	starttime := time.Now()
	// starttime2 := time.Now().UnixNano()
	go sender((queuetemp + strconv.Itoa(0)))
	// go conprod(1, "hello", "hello2")
	// go conprod(2, "hello2", "hello3")
	// go consumer("hello3")
	go conprodstarter(queuetemp)
	go consumer((queuetemp + strconv.Itoa(countprodcon)))

	<-finishedsending
	<-finishedconsumtion

	elapsed := time.Since(starttime)
	// endtime := float64((time.Now().UnixNano() - starttime2)) / float64(1000000)
	fmt.Printf("Elapsed time for sending and consuming: %s \nAveragetime per message: %s \n", elapsed, elapsed/time.Duration(messages))
	// fmt.Printf("Time gerechnet: %f \n", endtime)

	close(finishedsending)
	close(finishedconsumtion)

	println("Writing CSV file")
	// // write file
	output.Csv(csvStruct) //csvStruct is too large for a go routine
}

// starting and configuring a producer for rabbitmq
func sender(sendQueue string) {
	// connect to rabbitmq
	conn, err := amqp.Dial("amqp://rabbitmq:rabbitmq@localhost:5672/")
	// check for failover during connecting to rabbitmq
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()
	// connect o channel for API config
	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	//create queue for sending messages
	q, err := ch.QueueDeclare(
		sendQueue, // name
		true,     // durable
		false,     // delete when unused
		false,     // exclusive
		false,     // no-wait
		nil,       // arguments
	)
	failOnError(err, "Failed to declare a queue")

	//   body := "Hello World!"

	var testifleinput []byte
	var jsonMsg structs.MyInfo

	//   messageStartTime := time.Now()
	if testifleinput == nil {
		if messageSize != ""{
			testifleinput, err = ioutil.ReadFile(messageSize)
			if err != nil {
				fmt.Print(err)
			}	
		}
	}
	jsonMsg.ScareMe = "Yes, please"
	jsonMsg.Binaryfile = testifleinput
	var jsonOutput []byte
	var needTime int64

	//   jsonString := (string)(jsonOutput)

	// corrID := "myInfo"

	for i := 0; i < messages; i++ {

		corrID := "myinfo: " + strconv.Itoa(i)

		// select compression / message format
		switch compressionType {
		case "json":
			jsonMsg.TheTime = strconv.Itoa(int(time.Now().UnixNano())) //--> important to use this command twice, because of accourate time measurement !
			startTime := time.Now().UnixNano()
			jsonOutput, _ = json.Marshal(&jsonMsg)
			endTime := time.Now().UnixNano()

			duration:= endTime - startTime
			durationMs := float64(duration) / float64(1000000) //Nanosekunden in Milisekunden
			csvStruct.EncodingTime[0][i] = strconv.FormatFloat(durationMs, 'f', 6, 64)
			// println(jsonString)
		case "avro":
			jsonOutput, needTime = encoding.EncodeAvro(0, i, jsonMsg.ScareMe, jsonMsg.Binaryfile)
			durationMs := float64(needTime) / float64(1000000) //Nanosekunden in Milisekunden
			csvStruct.EncodingTime[0][i] = strconv.FormatFloat(durationMs, 'f', 6, 64)
		case "proto":
			jsonOutput, needTime = encoding.EncodeProto(0, i, jsonMsg.ScareMe, jsonMsg.Binaryfile)
			durationMs := float64(needTime) / float64(1000000) //Nanosekunden in Milisekunden
			csvStruct.EncodingTime[0][i] = strconv.FormatFloat(durationMs, 'f', 6, 64)
		default:
			jsonMsg.TheTime = strconv.Itoa(int(time.Now().UnixNano())) //--> important to use this command twice, because of accourate time measurement !
			jsonOutput, _ = json.Marshal(&jsonMsg)
			// println(jsonString)
		}

		messageStartTime := time.Now()
		err = ch.Publish(
			"",     // exchange
			q.Name, // routing key
			false,  // mandatory
			false,  // immediate
			amqp.Publishing{
				DeliveryMode: amqp.Persistent,
				ContentType:   "text/plain",
				CorrelationId: corrID,
				Body:          []byte(jsonOutput),
			})

		failOnError(err, "Failed to publish a message")

		messageEndTime := time.Since(messageStartTime).Seconds() * 1000
		if i < 3{
			fmt.Printf("Size of msg: %d \n", len(jsonOutput))
		}
		//   messageEndTimeTest := messageEndTime.Seconds()*1000
		csvStruct.SendTime[i] = strconv.FormatFloat(messageEndTime, 'f', 6, 64)
		//   completeTime = completeTime + messageEndTime
		//   log.Printf("SendTime: %f", messageEndTime)
		//   log.Printf("SendTimeduration for recerving Message %d: %s", i, strconv.FormatFloat(messageEndTime, 'f', 6, 64))

	}
	fmt.Printf("Set finishedsendding \n")
	finishedsending <- true
}

// stating and configureing a consumer for rabbitmq
func consumer(conQueue string) {
	conn, err := amqp.Dial("amqp://rabbitmq:rabbitmq@localhost:5672/")
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	q, err := ch.QueueDeclare(
		conQueue, // name
		true,    // durable
		false,    // delete when usused
		false,    // exclusive
		false,    // no-wait
		nil,      // arguments
	)
	failOnError(err, "Failed to declare a queue")

	// get messages
	msgs, err := ch.Consume( //kann alleine verwendet werden, da derzeit nach dem Konsumieren der Nachrichten diese gelöscht werden (bei persistenter Speicherung führt das zu Fehlerhaften Messungen)
		q.Name, // queue
		"",     // consumer
		true,   // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)

	i := 0
	for d := range msgs {
		messageReceivedTime := time.Now().UnixNano() // --> wenn die Zeit hier genommen wird, wird die Laufzeit der Forschleife mit eingerechnet
		if i < (messages) {
			if i == 0 {
				log.Printf("Received message %d with CorrID: %s", i, d.CorrelationId)
			}
			//   log.Printf("CorrID of Message %d: %s", i, d.CorrelationId)
			// log.Printf("Received a message: %s", d.Body)
			// println()

			var jsonRecord structs.MyInfo
			var needTime int64
			// fmt.Println(jsonRecord.TheTime)

			// check compressionType
			switch compressionType {
			case "json":
				// println(string(msg.Value))// --> falche Zeiten kommen hier bereits an !!!!!
				startTime := time.Now().UnixNano()
				json.Unmarshal(d.Body, &jsonRecord)
				endTime := time.Now().UnixNano()
	
				duration:= endTime - startTime
				durationMs := float64(duration) / float64(1000000) //Nanosekunden in Milisekunden
				csvStruct.DecodingTime[0][i] = strconv.FormatFloat(durationMs, 'f', 6, 64)	
			case "avro":
				// println(string(msg.Value)) // --> falche Zeiten kommen hier bereits an !!!!!
				jsonRecord,needTime = encoding.DecodeAvro(0, i, d.Body)
				durationMs := float64(needTime) / float64(1000000) //Nanosekunden in Milisekunden
				csvStruct.DecodingTime[0][i] = strconv.FormatFloat(durationMs, 'f', 6, 64)	
			case "proto":
				// println(string(msg.Value)) // --> falche Zeiten kommen hier bereits an !!!!!
				jsonRecord,needTime = encoding.DecodeProto(0, i, d.Body)
				durationMs := float64(needTime) / float64(1000000) //Nanosekunden in Milisekunden
				csvStruct.DecodingTime[0][i] = strconv.FormatFloat(durationMs, 'f', 6, 64)	
			}

			timevalue, err := strconv.ParseInt(jsonRecord.TheTime, 10, 64)
			if err != nil {
				log.Fatal("%s", err)
			}
			// set sessionStartTime (Time the first message (i==0) was send)
			if i == 0 {
				sessionStarttime = timevalue
			}

			//   currenttime:= int64(messageReceivedTime)
			//   fmt.Println("Current Time: ",messageReceivedTime)

			duration := messageReceivedTime - timevalue
			durationMs := float64(duration) / float64(1000000) //Nanosekunden in Milisekunden

			csvStruct.ConsumeTime[i] = strconv.FormatFloat(durationMs, 'f', 6, 64)
			// completeTime = completeTime + durationMs

			// compute complete time
			// correct the complete time --> complete time for sending and receiving != sum(sendtime + receivetime of all messages)
			sessionEndtime := time.Now().UnixNano()
			sessionEndtimeMS := float64(sessionEndtime) / float64(1000000)     //Nanosekunden in Milisekunden
			sessionStarttimeMS := float64(sessionStarttime) / float64(1000000) //Nanosekunden in Milisekunden
			sendReceiveDuration := sessionEndtimeMS - sessionStarttimeMS
			csvStruct.CompleteTime = sendReceiveDuration

			// fmt.Printf("Duration of Message: %d \n", duration)
			// fmt.Printf("Duration of Message in ms: %f \n", durationMs)
			// fmt.Printf("CompleteTime: %f \n", completeTime)

			// println()

			i = i + 1
		}
		if i == (messages) {
			break
		}
	}

	failOnError(err, "Failed to register a consumer")
	fmt.Printf("Set finishedconsuming \n")
	finishedconsumtion <- true
}

// stating a producer and consumer for rabbitmq to simulate multiple message exchanges over the message bus
func conprod(consendID int, conQueueName string, prodQueueName string) {
	conn, err := amqp.Dial("amqp://rabbitmq:rabbitmq@localhost:5672/")
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	// Consumer channel and queue
	chC, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer chC.Close()

	qc, err := chC.QueueDeclare(
		conQueueName, // name
		true,        // durable
		false,        // delete when usused
		false,        // exclusive
		false,        // no-wait
		nil,          // arguments
	)
	failOnError(err, "Failed to declare a queue")

	// producer channel and queue
	chP, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer chP.Close()

	//create queue for sending messages
	qp, err := chP.QueueDeclare(
		prodQueueName, // name
		false,         // durable
		false,         // delete when unused
		false,         // exclusive
		false,         // no-wait
		nil,           // arguments
	)
	failOnError(err, "Failed to declare a queue")

	// get messages
	msgs, err := chC.Consume( //kann alleine verwendet werden, da derzeit nach dem Konsumieren der Nachrichten diese gelöscht werden (bei persistenter Speicherung führt das zu Fehlerhaften Messungen)
		qc.Name, // queue
		"",      // consumer
		true,    // auto-ack
		false,   // exclusive
		false,   // no-local
		false,   // no-wait
		nil,     // args
	)

	i := 0
	for d := range msgs {
		if d.MessageCount >= 0 {
			messageReceivedTime := time.Now().UnixNano() // --> wenn die Zeit hier genommen wird, wird die Laufzeit der Forschleife mit eingerechnet
			if i < (messages) {
				if i == 0 {
					log.Printf("Received message %d with CorrID: %s", i, d.CorrelationId)
					log.Printf("ConProd %d received CorrID of Message %d: %s", consendID, i, d.CorrelationId)
				}
				//   log.Printf("Received a message: %s", d.Body)
				// println()

				var jsonRecord structs.MyInfo
				var jsonOutput []byte
				var needTime int64
				// fmt.Println(jsonRecord.TheTime)

				// check compressionType
				switch compressionType {
				case "json":
					startTime := time.Now().UnixNano()
					json.Unmarshal(d.Body, &jsonRecord)
					endTime := time.Now().UnixNano()
		
					duration:= endTime - startTime
					durationMs := float64(duration) / float64(1000000) //Nanosekunden in Milisekunden
					csvStruct.DecodingTime[consendID][i] = strconv.FormatFloat(durationMs, 'f', 6, 64)		
				case "avro":
					jsonRecord, needTime = encoding.DecodeAvro(consendID, i, d.Body)
					durationMs := float64(needTime) / float64(1000000) //Nanosekunden in Milisekunden
					csvStruct.DecodingTime[consendID][i] = strconv.FormatFloat(durationMs, 'f', 6, 64)		
				case "proto":
					jsonRecord, needTime = encoding.DecodeProto(consendID, i, d.Body)
					durationMs := float64(needTime) / float64(1000000) //Nanosekunden in Milisekunden
					csvStruct.DecodingTime[consendID][i] = strconv.FormatFloat(durationMs, 'f', 6, 64)		
				}

				timevalue, err := strconv.ParseInt(jsonRecord.TheTime, 10, 64)
				if err != nil {
					log.Fatal("%s", err)
				}
				//   fmt.Println(timevalue)

				//   currenttime:= int64(messageReceivedTime)
				//   fmt.Println("Current Time: ",messageReceivedTime)

				duration := messageReceivedTime - timevalue
				durationMs := float64(duration) / float64(1000000) //Nanosekunden in Milisekunden

				csvStruct.ConSendTime[consendID][i] = strconv.FormatFloat(durationMs, 'f', 6, 64)
				//    completeTime = completeTime + durationMs

				//   fmt.Printf("Duration of Message: %d \n", duration)
				//   fmt.Printf("Duration of Message in ms: %f \n", durationMs)
				//   fmt.Printf("CompleteTime: %f \n", completeTime)
				//   println()

				// sending message with timechange

				// corrID := "myinfo2: " + strconv.Itoa(i)
				corrID := "myinfo" + strconv.Itoa(consendID) + ": " + strconv.Itoa(i)

				// select compression / message format
				switch compressionType {
				case "":
					jsonRecord.TheTime = strconv.Itoa(int(time.Now().UnixNano()))
					startTime := time.Now().UnixNano()
					jsonOutput, _ = json.Marshal(&jsonRecord)
					endTime := time.Now().UnixNano()
		
					duration:= endTime - startTime
					durationMs := float64(duration) / float64(1000000) //Nanosekunden in Milisekunden
					csvStruct.EncodingTime[consendID][i] = strconv.FormatFloat(durationMs, 'f', 6, 64)		
				case "avro":
					jsonOutput, needTime = encoding.EncodeAvro(consendID, i, jsonRecord.ScareMe, jsonRecord.Binaryfile)
					durationMs := float64(needTime) / float64(1000000) //Nanosekunden in Milisekunden
					csvStruct.EncodingTime[consendID][i] = strconv.FormatFloat(durationMs, 'f', 6, 64)		
				case "proto":
					jsonOutput, needTime = encoding.EncodeProto(consendID, i, jsonRecord.ScareMe, jsonRecord.Binaryfile)
					durationMs := float64(needTime) / float64(1000000) //Nanosekunden in Milisekunden
					csvStruct.EncodingTime[consendID][i] = strconv.FormatFloat(durationMs, 'f', 6, 64)		
				default:
					jsonRecord.TheTime = strconv.Itoa(int(time.Now().UnixNano()))
					jsonOutput, _ = json.Marshal(&jsonRecord)
				}

				messageStartTime := time.Now()
				err = chP.Publish(
					"",      // exchange
					qp.Name, // routing key
					false,   // mandatory
					false,   // immediate
					amqp.Publishing{
						DeliveryMode: amqp.Persistent,
						ContentType:   "text/plain",
						CorrelationId: corrID,
						Body:          []byte(jsonOutput),
					})

				failOnError(err, "Failed to publish a message")

				messageEndTime := time.Since(messageStartTime).Seconds() * 1000
				//   messageEndTimeTest := messageEndTime.Seconds()*1000
				//   sendTime[i] = strconv.FormatFloat(messageEndTime, 'f', 6, 64)
				csvStruct.ConSendTime[consendID][i] = strconv.FormatFloat((durationMs + messageEndTime), 'f', 6, 64)
				// completeTime = completeTime + messageEndTime
				//   log.Printf("SendTime: %f", messageEndTime)
				//   log.Printf("SendTimeduration for recerving Message %d: %s", i, strconv.FormatFloat(messageEndTime, 'f', 6, 64))
				i = i + 1
			}

			if i == (messages) {
				break
			}

		}
	}
}

// helper function to configure and start several instances of the consumer/producer processes
func conprodstarter(queuetmplate string) {
	queuetemp := queuetmplate
	for i := 1; i <= countprodcon; i++ {
		go conprod(i, (queuetemp + strconv.Itoa(i-1)), (queuetemp + strconv.Itoa(i)))
	}
}

// helper function to log errors
func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}