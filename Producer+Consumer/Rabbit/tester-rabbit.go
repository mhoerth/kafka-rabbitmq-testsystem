package main

import (
	// "os"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"strconv"
	"time"
	"log"

	/*  "../../src/mdp/segmenthelper"
	 */
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

func main(){

	messages = 10
	starttime := time.Now()
	go sender()
	go consumer()

	<-finishedsending
	<-finishedconsumtion

	elapsed := time.Since(starttime)
	fmt.Printf("Elapsed time for sending and consuming: %s \nAveragetime per message: %s \n", elapsed, elapsed/time.Duration(messages))

	close(finishedsending)
	close(finishedconsumtion)

}

func sender(){
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
		"hello", // name
		false,   // durable
		false,   // delete when unused
		false,   // exclusive
		false,   // no-wait
		nil,     // arguments
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
	  jsonMsg.TheTime = strconv.Itoa(int(time.Now().Unix()))
	  jsonMsg.ScareMe = "Yes, please"
	  jsonMsg.Binaryfile = testifleinput

	  jsonOutput, _ := json.Marshal(&jsonMsg)
	//   jsonString := (string)(jsonOutput)

	  for i:=0; i<messages; i++{
		err = ch.Publish(
			"",     // exchange
			q.Name, // routing key
			false,  // mandatory
			false,  // immediate
			amqp.Publishing {
			  ContentType: "text/plain",
			  CorrelationId: "myinfo",
			  Body:        []byte(jsonOutput),
			})
			
		  failOnError(err, "Failed to publish a message")
	  }

	  finishedsending <- true
}

func consumer(){
	conn, err := amqp.Dial("amqp://rabbitmq:rabbitmq@localhost:5672/")
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	q, err := ch.QueueDeclare(
	"hello", // name
	false,   // durable
	false,   // delete when usused
	false,   // exclusive
	false,   // no-wait
	nil,     // arguments
	)
	failOnError(err, "Failed to declare a queue")

	// get messages
	// for i:=0; i<messages; i++{ //benötigt mehr Zeit mit der Schleife, da immer ein neuer channel erstellt werden muss!!!
		ch.Consume(		//kann alleine verwendet werden, da derzeit nach dem Konsumieren der Nachrichten diese gelöscht werden (bei persistenter Speicherung führt das zu Fehlerhaften Messungen)
			q.Name, // queue
			"",     // consumer
			true,   // auto-ack
			false,  // exclusive
			false,  // no-local
			false,  // no-wait
			nil,    // args
		  )
	// }

	//printing all received messages (ammount of send messages by the sender!)

	// msgs, err := ch.Consume(
	// 	q.Name, // queue
	// 	"",     // consumer
	// 	true,   // auto-ack
	// 	false,  // exclusive
	// 	false,  // no-local
	// 	false,  // no-wait
	// 	nil,    // args
	//   )
	  
	//   i:= 0

	// for d := range msgs {
	// 	if(i < messages - 1){//messages -1 because i is counting form 0 to 9
	// 		// log.Printf("Received a message: %s", d.Body)
	// 		// log.Printf("Received a message: %s", d.CorrelationId)
	// 		i = i+1
	// 	}else{
	// 		println("leaving routine")
	// 		break
	// 	}
	// }
	
	failOnError(err, "Failed to register a consumer")

	  finishedconsumtion <- true
}

func failOnError(err error, msg string) {
	if err != nil {
	  log.Fatalf("%s: %s", msg, err)
	}
}
