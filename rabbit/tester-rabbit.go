package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"time"

	/*  "../../src/mdp/segmenthelper"
	 */
	"github.com/golang/protobuf/proto"
	"github.com/linkedin/goavro"
	"github.com/streadway/amqp"
)

type MyInfo struct {
	TheTime    string `json:"theTime"`
	ScareMe    string `json:"scare"`
	Binaryfile []byte `json:"binary"`
}

var finishedconsumtion = make(chan bool, 10000)
var finishedsending = make(chan bool, 10000)
var messages int
var countprodcon int

var sendTime [10000]string
var consendTime [6][10000]string
var consumeTime [10000]string
var completeTime float64
var sessionStarttime int64

const loginEventAvroSchema = `{"type": "record", "name": "LoginEvent", "fields": [{"name": "TheTime", "type": "string"}, {"name": "ScareMe", "type": "string"}, {"name": "Binaryfile", "type": "bytes"}]}`

var compressionType string

func main() {

	messages = 10
	countprodcon = 2
	queuetemp := "hello"

	// fill commandline parameters in programm variables
	for i := 0; i < len(os.Args); i++ {
		fmt.Printf("%d : %s \n", i, os.Args[i])

		if i == 1 {
			messagesCMD, err := strconv.ParseInt(os.Args[i], 10, 64)
			if err != nil {
				log.Fatal("%s", err)
			}
			messages = int(messagesCMD)
		}
		if i == 2 {
			queuetemp = os.Args[i]
		}
		if i == 3 {
			countprodconCMD, err := strconv.ParseInt(os.Args[i], 10, 64)
			if err != nil {
				log.Fatal("%s", err)
			}
			countprodcon = int(countprodconCMD)
		}
		if i == 4 {
			compressionType = os.Args[i]
		}
	}

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
	// write file

	f, err := os.Create("testfileRabbit.csv")
	if err != nil {
		panic(err)
	}
	defer f.Close()

	f.WriteString("Messagenumber;")
	f.WriteString("ProducerSendTime;")
	f.WriteString("Consumer&ProducerSendTime;")
	f.WriteString("ConsumerTime;\n")

	for i := 0; i < messages; i++ {
		// f.WriteString("ProducerSendTime: ")
		f.WriteString(strconv.Itoa(i) + " ;") //Messagenumber
		f.WriteString(sendTime[i] + ";")
		for cp := 1; cp <= countprodcon; cp++ {
			// f.WriteString("Consumer&ProducerSendTime: ")
			// f.WriteString(";")
			f.WriteString(consendTime[cp][i])
			if cp < countprodcon {
				f.WriteString("; \n" + strconv.Itoa(i) + "; ;")
			} else {
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

}

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
		false,     // durable
		false,     // delete when unused
		false,     // exclusive
		false,     // no-wait
		nil,       // arguments
	)
	failOnError(err, "Failed to declare a queue")

	//   body := "Hello World!"

	var testifleinput []byte
	var jsonMsg MyInfo

	//   messageStartTime := time.Now()
	if testifleinput == nil {
		testifleinput, err = ioutil.ReadFile("../../output-1Kibi-rand")
		if err != nil {
			fmt.Print(err)
		}
	}
	jsonMsg.ScareMe = "Yes, please"
	jsonMsg.Binaryfile = testifleinput
	var jsonOutput []byte

	//   jsonString := (string)(jsonOutput)

	// corrID := "myInfo"

	for i := 0; i < messages; i++ {

		corrID := "myinfo: " + strconv.Itoa(i)

		// select compression / message format
		switch compressionType {
		case "":
			jsonMsg.TheTime = strconv.Itoa(int(time.Now().UnixNano())) //--> important to use this command twice, because of accourate time measurement !
			jsonOutput, _ = json.Marshal(&jsonMsg)
			// println(jsonString)
		case "avro":
			jsonOutput = encodeAvro(jsonMsg.ScareMe, jsonMsg.Binaryfile)
		case "proto":
			jsonOutput = encodeProto(jsonMsg.ScareMe, jsonMsg.Binaryfile)
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
				ContentType:   "text/plain",
				CorrelationId: corrID,
				Body:          []byte(jsonOutput),
			})

		failOnError(err, "Failed to publish a message")

		messageEndTime := time.Since(messageStartTime).Seconds() * 1000
		//   messageEndTimeTest := messageEndTime.Seconds()*1000
		sendTime[i] = strconv.FormatFloat(messageEndTime, 'f', 6, 64)
		//   completeTime = completeTime + messageEndTime
		//   log.Printf("SendTime: %f", messageEndTime)
		//   log.Printf("SendTimeduration for recerving Message %d: %s", i, strconv.FormatFloat(messageEndTime, 'f', 6, 64))

	}
	fmt.Printf("Set finishedsendding \n")
	finishedsending <- true
}

func consumer(conQueue string) {
	conn, err := amqp.Dial("amqp://rabbitmq:rabbitmq@localhost:5672/")
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	q, err := ch.QueueDeclare(
		conQueue, // name
		false,    // durable
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

			var jsonRecord MyInfo
			// fmt.Println(jsonRecord.TheTime)

			// check compressionType
			switch compressionType {
			case "":
				// println(string(msg.Value))// --> falche Zeiten kommen hier bereits an !!!!!
				json.Unmarshal(d.Body, &jsonRecord)
			case "avro":
				// println(string(msg.Value)) // --> falche Zeiten kommen hier bereits an !!!!!
				jsonRecord = decodeAvro(d.Body)
			case "proto":
				// println(string(msg.Value)) // --> falche Zeiten kommen hier bereits an !!!!!
				jsonRecord = decodeProto(d.Body)
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

			consumeTime[i] = strconv.FormatFloat(durationMs, 'f', 6, 64)
			// completeTime = completeTime + durationMs

			// compute complete time
			// correct the complete time --> complete time for sending and receiving != sum(sendtime + receivetime of all messages)
			sessionEndtime := time.Now().UnixNano()
			sessionEndtimeMS := float64(sessionEndtime) / float64(1000000)     //Nanosekunden in Milisekunden
			sessionStarttimeMS := float64(sessionStarttime) / float64(1000000) //Nanosekunden in Milisekunden
			sendReceiveDuration := sessionEndtimeMS - sessionStarttimeMS
			completeTime = sendReceiveDuration

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
		false,        // durable
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

				var jsonRecord MyInfo
				var jsonOutput []byte
				// fmt.Println(jsonRecord.TheTime)

				// check compressionType
				switch compressionType {
				case "":
					json.Unmarshal(d.Body, &jsonRecord)
				case "avro":
					jsonRecord = decodeAvro(d.Body)
				case "proto":
					jsonRecord = decodeProto(d.Body)
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

				consendTime[consendID][i] = strconv.FormatFloat(durationMs, 'f', 6, 64)
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
					jsonOutput, _ = json.Marshal(&jsonRecord)
				case "avro":
					jsonOutput = encodeAvro(jsonRecord.ScareMe, jsonRecord.Binaryfile)
				case "proto":
					jsonOutput = encodeProto(jsonRecord.ScareMe, jsonRecord.Binaryfile)
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
						ContentType:   "text/plain",
						CorrelationId: corrID,
						Body:          []byte(jsonOutput),
					})

				failOnError(err, "Failed to publish a message")

				messageEndTime := time.Since(messageStartTime).Seconds() * 1000
				//   messageEndTimeTest := messageEndTime.Seconds()*1000
				//   sendTime[i] = strconv.FormatFloat(messageEndTime, 'f', 6, 64)
				consendTime[consendID][i] = strconv.FormatFloat((durationMs + messageEndTime), 'f', 6, 64)
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

func conprodstarter(queuetmplate string) {
	queuetemp := queuetmplate
	for i := 1; i <= countprodcon; i++ {
		go conprod(i, (queuetemp + strconv.Itoa(i-1)), (queuetemp + strconv.Itoa(i)))
	}
}

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

func encodeAvro(scareMe string, binary []byte) []byte {
	// println("avro compression starting....")
	var b bytes.Buffer
	// foo := bufio.NewWriter(&b)

	codec, err := goavro.NewCodec(loginEventAvroSchema)
	if err != nil {
		panic(err)
	}

	var values []map[string]interface{}
	theTime := strconv.Itoa(int(time.Now().UnixNano()))
	// values = append(values, m)
	// values = append(values, map[string] interface{}{"TheTime": "643573245", "ScareMe": "scareMe", "Binaryfile": []byte{0, 1, 2, 4, 5}})
	// values = append(values, map[string] interface{}{"TheTime": "657987654", "ScareMe": "scareMe", "Binaryfile": []byte{0, 1, 2, 4, 5}})
	values = append(values, map[string]interface{}{"TheTime": theTime, "ScareMe": scareMe, "Binaryfile": binary})

	// f, err := os.Create("event.avro")
	// if err != nil {
	// 	panic(err)
	// }

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

	bytearray := b.Bytes()
	// control printout
	// println(b.Cap())
	// fmt.Printf("Time: %s \n", jsonMsg.TheTime)
	// fmt.Printf("ScareMe: %s \n", jsonMsg.ScareMe)
	// fmt.Printf("Binary: %s \n", jsonMsg.Binaryfile)

	// println("avro compression finished!!!")

	return bytearray
}

func decodeAvro(message []byte) MyInfo {
	var output MyInfo
	b := bytes.NewBuffer(message)

	// decode this shit to verify
	ocfr, err := goavro.NewOCFReader(b)

	var decoded interface{}
	// println(ocfr.Scan()) //--> if true, messages available

	for ocfr.Scan() {
		decoded, err = ocfr.Read()
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s\n", err)
		}
		// fmt.Println(decoded)
		// fmt.Printf("Test mit Printf: %v \n", decoded)
	}

	jsontest, err := json.Marshal(decoded)
	if err != nil {
		log.Fatal(err)
	}
	// println(string(jsontest))
	json.Unmarshal(jsontest, &output)

	// fmt.Printf("Myinfo1: %s \n", output)
	// fmt.Printf("Time: %s \n", output.TheTime)
	// fmt.Printf("ScareMe: %s \n", output.ScareMe)
	// fmt.Printf("Binary: %s \n", output.Binaryfile)

	return output
}

func encodeProto(scareMe string, binary []byte) []byte {
	// get current time
	getTime := strconv.Itoa(int(time.Now().UnixNano()))

	// define object content
	object := &TestProto{
		TheTime:    getTime,
		ScareMe:    scareMe,
		BinaryFile: binary,
	}

	// marshal content like json format
	data, err := proto.Marshal(object)
	if err != nil {
		log.Fatal("marshaling error: ", err)
	}

	// printing out our raw protobuf object
	// fmt.Println(data)

	return data
}

func decodeProto(message []byte) MyInfo {
	// byte array into an object which can be modified and used
	object := &TestProto{}
	err := proto.Unmarshal(message, object)
	if err != nil {
		log.Fatal("unmarshaling error: ", err)
	}

	// protobuf to json
	var jsonMsg MyInfo

	jsonMsg.TheTime = object.GetTheTime()
	jsonMsg.ScareMe = object.GetScareMe()
	jsonMsg.Binaryfile = object.GetBinaryFile()

	// print out our `newElliot` object
	// for good measure
	// fmt.Printf("Time: %s \n", jsonMsg.TheTime)
	// fmt.Printf("ScareMe: %s \n", jsonMsg.ScareMe)
	// fmt.Printf("Binary: %s \n", jsonMsg.Binaryfile)

	return jsonMsg
}
