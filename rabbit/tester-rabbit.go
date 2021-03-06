package rabbit

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"strconv"
	"time"
	"os/exec"

	"mom-test/encoding"
	"mom-test/structs"
	"mom-test/output"

	// "encoding/base64"

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
func Rabbit(interations int, messageamount int, queue string, conProdInst int, compression string, sizeOfMessaage string, delayTime int) {

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
	csvStruct.MsDelay = delayTime

	for i:=0; i<interations; i++{

		cpuStart, err := exec.Command("bash", "-c", "cat /proc/stat |grep cpu").Output()
		if err != nil {
			panic(err)
		}

		csvStruct.Interation = i
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
		
		// delete queues
		// deleteQueues(queuetemp, countprodcon)

		// compute RoundTripTime
		println("Compute RTT and ConsumeTime")
		csvStruct.RoundTripTime = output.ComputeRoundTripTime(csvStruct.SendTimeStamps, csvStruct.ConsumeTimeStamps, csvStruct.Messages)
		csvStruct.ConsumeTime = output.ComputeConsumeTime(csvStruct.EncodingTime, csvStruct.SendTime, csvStruct.ConsumeTime, csvStruct.CountProdCon, csvStruct.Messages)
		

		cpuStop, err := exec.Command("bash", "-c", "cat /proc/stat |grep cpu").Output()
		if err != nil {
			panic(err)
		}

		println("Writing CSV file")
		// // write file
		output.Csv(csvStruct, cpuStart, cpuStop) //csvStruct is too large for a go routine	
		time.Sleep(1000000000) //wait 1 second before next testexecution
	}
}
// CloseChannels is used channels, needed for the feature to do multiple interations with he same config provided with one call out of the main function
func CloseChannels(){
	close(finishedsending)
	close(finishedconsumtion)
}

// work in progress, currently queues are deleted automatically if not used anymore
func deleteQueues(queuetemp string, queues int){
		// connect to rabbitmq
		conn, err := amqp.Dial("amqp://rabbitmq:rabbitmq@localhost:5672/")
		// check for failover during connecting to rabbitmq
		failOnError(err, "Failed to connect to RabbitMQ")
		defer conn.Close()
		// connect o channel for API config
		ch, err := conn.Channel()
		failOnError(err, "Failed to open a channel")
		defer ch.Close()

		queue:= queuetemp

			for i:=0; i<=queues; i++{
				queue = queue + strconv.Itoa(i)
				println(queue)
				for {
				//delete queue for sending messages
				_, err := ch.QueueDelete(
					queue, // name
					true,     // ifUnused
					true,     // if empty
					true,     // noWait
				)
				// failOnError(err, "Failed to delete a queue")
				if (err != nil){
					fmt.Printf("Queue deleted: %s \n", queue)
					break
				}else{
					println("Wait a second")
					time.Sleep(1000000000) //wait 1 second before next testexecution
				}
			}
		}

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
		true,     // delete when unused
		false,     // exclusive
		false,     // no-wait
		nil,       // arguments
	)
	failOnError(err, "Failed to declare a queue")

	//   body := "Hello World!"

	var testifleinput []byte
	var jsonMsg structs.MyInfo
	var jsonOutput []byte

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
	jsonOutput, _ = json.Marshal(&jsonMsg)
	// fmt.Printf("Size of msg(scareMe): %d \n", len(jsonOutput))

	jsonMsg.Binaryfile = testifleinput
	jsonOutput, _ = json.Marshal(&jsonMsg)
	// fmt.Printf("Size of msg(ScareMe + Binaryfile): %d \n", len(jsonOutput))

	var startTime int64

	//   jsonString := (string)(jsonOutput)

	// corrID := "myInfo"

	for i := 0; i < messages; i++ {

		corrID := "myinfo: " + strconv.Itoa(i)

		// wait for each send interval --> create a fixed transfer rate of messages
		if i != 0{
			// if (i % 100) == 0{
				// println("Wait 100 msec before sending again 100 messages")
				time.Sleep(time.Duration(csvStruct.MsDelay) * time.Millisecond)
			// }
		}

		// select compression / message format
		switch compressionType {
		case "json":
			jsonMsg.TheTime = strconv.Itoa(int(time.Now().UnixNano())) //--> important to use this command twice, because of accourate time measurement !
			startTime = time.Now().UnixNano()
			jsonOutput, _ = json.Marshal(&jsonMsg)
			// fmt.Printf("Size of msg: %d \n", len(jsonOutput))

			// endTime := time.Now().UnixNano()

			// println(jsonString)
		case "avro":
			jsonOutput, startTime = encoding.EncodeAvro(0, i, jsonMsg.ScareMe, jsonMsg.Binaryfile)
		case "proto":
			jsonOutput, startTime = encoding.EncodeProto(0, i, jsonMsg.ScareMe, jsonMsg.Binaryfile)
		}

		messageStartTime := time.Now()

		// write encodingTime
		duration:= messageStartTime.UnixNano() - startTime
		durationMs := float64(duration) / float64(1000000) //Nanosekunden in Milisekunden
		csvStruct.EncodingTime[0][i] = strconv.FormatFloat(durationMs, 'f', 6, 64)

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
		
		// set start of the round trip time
		csvStruct.SendTimeStamps[i] = strconv.FormatInt(messageStartTime.UnixNano(), 10)

		// for testing the impact of different messagesizes
		// var jsonMsg2 structs.MyInfo
		if i < 3{
			// verify length of every single value in the json object
			// fmt.Printf("Size of jsonMsg.ScareMe: %d \n", len(jsonMsg.ScareMe))
			// fmt.Printf("Size of jsonMsg.Binaryfile: %d \n", len(jsonMsg.Binaryfile))
			// fmt.Printf("Size of jsonMsg.TheTime: %d \n", len(jsonMsg.TheTime))
			fmt.Printf("Size of msg: %d \n", len(jsonOutput))
			csvStruct.Filesize = int64(len(jsonOutput))
			// print the json object
			// println(string(jsonOutput))
			// fmt.Printf("Size of msg (string): %d \n", len(string(jsonOutput)))

			// test messagesize with base64 encoding (json marshall encodes []byte to base64 string)
			// jsonOutput, _ = json.Marshal(&jsonMsg2)
			// fmt.Printf("Testoutput message without any information: %d \n", len(jsonOutput))
			// println(string(jsonOutput))

			// jsonMsg2.Binaryfile = testifleinput
			// // println(string(testifleinput))
			// println(len(testifleinput))
			// println()
			// sEnc := base64.StdEncoding.EncodeToString(testifleinput)
			// // fmt.Println(sEnc)
			// println(len(sEnc))
			
			// jsonOutput, _ = json.Marshal(&jsonMsg2)
			// fmt.Printf("Testoutput message only with binaryfile: %d \n", len(jsonOutput))
			// println(string(jsonOutput))

		}
		//   messageEndTimeTest := messageEndTime.Seconds()*1000
		csvStruct.SendTime[0][i] = strconv.FormatFloat(messageEndTime, 'f', 6, 64)
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
		true,    // delete when usused
		false,    // exclusive
		false,    // no-wait
		nil,      // arguments
	)
	failOnError(err, "Failed to declare a queue")

// set QoS
	err = ch.Qos(
		1,		//prefetch count
		0,		//prefetch size
	false,		//global
	)
	failOnError(err, "Failed to set QoS")

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
		csvStruct.ConsumeTimeStamps[i] = strconv.FormatInt(messageReceivedTime, 10)

		if i < (messages) {
			if i == 0 {
				log.Printf("Received message %d with CorrID: %s", i, d.CorrelationId)
			}
			//   log.Printf("CorrID of Message %d: %s", i, d.CorrelationId)
			// log.Printf("Received a message: %s", d.Body)
			// println()

			var jsonRecord structs.MyInfo
			var endTime int64
			// fmt.Println(jsonRecord.TheTime)

			// check compressionType
			switch compressionType {
			case "json":
				// println(string(msg.Value))// --> falche Zeiten kommen hier bereits an !!!!!
				// startTime := time.Now().UnixNano()
				json.Unmarshal(d.Body, &jsonRecord)
				endTime = time.Now().UnixNano()
	
			case "avro":
				// println(string(msg.Value)) // --> falche Zeiten kommen hier bereits an !!!!!
				jsonRecord,endTime = encoding.DecodeAvro(0, i, d.Body)
			case "proto":
				// println(string(msg.Value)) // --> falche Zeiten kommen hier bereits an !!!!!
				jsonRecord,endTime = encoding.DecodeProto(0, i, d.Body)
			}

			// write decodingTime
			duration:= endTime - messageReceivedTime
			durationMs := float64(duration) / float64(1000000) //Nanosekunden in Milisekunden
			csvStruct.DecodingTime[0][i] = strconv.FormatFloat(durationMs, 'f', 6, 64)	

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

			duration = messageReceivedTime - timevalue
			durationMs = float64(duration) / float64(1000000) //Nanosekunden in Milisekunden

			csvStruct.ConsumeTime[csvStruct.CountProdCon][i] = strconv.FormatFloat(durationMs, 'f', 6, 64)
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
		true,        // delete when usused
		false,        // exclusive
		false,        // no-wait
		nil,          // arguments
	)
	failOnError(err, "Failed to declare a queue")

	// set QoS
	err = chC.Qos(
		1,		//prefetch count
		0,		//prefetch size
	false,		//global
	)
	failOnError(err, "Failed to set QoS")

	// producer channel and queue
	chP, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer chP.Close()

	//create queue for sending messages
	qp, err := chP.QueueDeclare(
		prodQueueName, // name
		true,         // durable
		true,         // delete when unused
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
				var endTime int64
				var startTime int64
				// fmt.Println(jsonRecord.TheTime)

				// check compressionType
				switch compressionType {
				case "json":
					// startTime := time.Now().UnixNano()
					json.Unmarshal(d.Body, &jsonRecord)
					endTime = time.Now().UnixNano()
		
				case "avro":
					jsonRecord, endTime = encoding.DecodeAvro(consendID, i, d.Body)
				case "proto":
					jsonRecord, endTime = encoding.DecodeProto(consendID, i, d.Body)
				}

				// write decodingTime
				duration:= endTime - messageReceivedTime
				durationMs := float64(duration) / float64(1000000) //Nanosekunden in Milisekunden
				csvStruct.DecodingTime[consendID][i] = strconv.FormatFloat(durationMs, 'f', 6, 64)		

				timevalue, err := strconv.ParseInt(jsonRecord.TheTime, 10, 64)
				if err != nil {
					log.Fatal("%s", err)
				}
				//   fmt.Println(timevalue)

				//   currenttime:= int64(messageReceivedTime)
				//   fmt.Println("Current Time: ",messageReceivedTime)

				duration = messageReceivedTime - timevalue
				durationMs = float64(duration) / float64(1000000) //Nanosekunden in Milisekunden

				csvStruct.ConsumeTime[consendID -1][i] = strconv.FormatFloat(durationMs, 'f', 6, 64)
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
				case "json":
					jsonRecord.TheTime = strconv.Itoa(int(time.Now().UnixNano()))
					startTime = time.Now().UnixNano()
					jsonOutput, _ = json.Marshal(&jsonRecord)
					// endTime := time.Now().UnixNano()
		
				case "avro":
					jsonOutput, startTime = encoding.EncodeAvro(consendID, i, jsonRecord.ScareMe, jsonRecord.Binaryfile)
				case "proto":
					jsonOutput, startTime = encoding.EncodeProto(consendID, i, jsonRecord.ScareMe, jsonRecord.Binaryfile)
				}

				// // wait for each send interval --> create a fixed transfer rate of messages
				// if i != 0{
				// 	// if (i % 100) == 0{
				// 		// println("Wait 100 msec before sending again 100 messages")
				// 		time.Sleep(time.Duration(csvStruct.MsDelay) * time.Millisecond)
				// 		// }
				// }

				messageStartTime := time.Now()

				// write encodingTime
				duration = messageStartTime.UnixNano() - startTime
				durationMs  = float64(duration) / float64(1000000) //Nanosekunden in Milisekunden
				csvStruct.EncodingTime[consendID][i] = strconv.FormatFloat(durationMs, 'f', 6, 64)	

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
				csvStruct.SendTime[consendID][i] = strconv.FormatFloat( messageEndTime, 'f', 6, 64)
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