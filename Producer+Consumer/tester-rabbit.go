package main

import (
	// "os"
	// "encoding/json"
	// "fmt"
	// "io/ioutil"
	// "strconv"
	// "time"
	"log"

	/*  "../../src/mdp/segmenthelper"
	 */
	"github.com/streadway/amqp"
)

func main(){
	go sender()
	go consumer()
	for{
		println("Waiting for action")
	}
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
	  
	  body := "Hello World!"
	  err = ch.Publish(
		"",     // exchange
		q.Name, // routing key
		false,  // mandatory
		false,  // immediate
		amqp.Publishing {
		  ContentType: "text/plain",
		  Body:        []byte(body),
		})
	  failOnError(err, "Failed to publish a message")
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
	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		true,   // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	  )
	  failOnError(err, "Failed to register a consumer")
	  
	  forever := make(chan bool)
	  
	  go func() {
		for d := range msgs {
		  log.Printf("Received a message: %s", d.Body)
		}
	  }()
	  
	  log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	  <-forever
}

func failOnError(err error, msg string) {
	if err != nil {
	  log.Fatalf("%s: %s", msg, err)
	}
}
