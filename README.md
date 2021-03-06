# Kafka + RabbitMQ testsystem

This repository contains a testsystem for messagebus systems (currently Apache kafka and RabbitMQ), which is used in my bachelor thesis.  

# Repository Description
- contains bouth kafka and rabbitmq testfiles
- [env](#env), contains the docker-compose files for the envronment
    - included are docker-compose files for starting 1 or 3 kafka / rabbitmq instances
    - special for rabbitmq is currently the helper.sh to connect the rabbitmq instnces to a cluster
- [kafka](#kafka), contains one testfile for kafka
    - a test implementation of a consumergroup is included, but currently not integrated in the main function, therefore not accessable during runtime
- [rabbit](#rabbit), contains one testfile for rabbitmq
- [encoding](#encoding), contains encoding options
    - protobuf
    - avro
- [output](#output), contains the output creation (csv file)
- [structs](#structs), contains the used structs for testing (json struct and csv struct)
- [testfiles](#testfiles), contains the used testfiles (binary files) for testing and the go file to create the testfiles
- [testing](#testing), contains a basic kafka producer and consumer

# Requirements
On the basis of  golang this testsystem needs a few other components to interact with the different bus systems and to use different encoding formats
- docker client version 18.09.7 or higher
- docker server (docker engine) version 19.03.5 or higher
- [Sarama kafka client](https://github.com/Shopify/sarama) --> kafka interaction
- [AMQP 0-9-1](https://github.com/streadway/amqp) --> rabbitmq interaction
- [goavro](https://github.com/linkedin/goavro) --> avro encoding
- [protobuf](https://github.com/golang/protobuf/proto) --> protobuf encoding

# Build
- Switch into this repository and execute "go build"

# Test execution
Important notice:
This testsystem is designed to measure the performance of the different systems. Therefore, the standard configuration is used, which means that the standard IPs and Ports are used also. (Kafka: 127.0.0.1:9092) (RabbitMQ: amqp://rabbitmq:rabbitmq@localhost:5672/)  
If you are using the provided docker-compose files within this repo nothing should be needed to reconfigure.

First:
Run at least one of the docker-compose files to start at least one bus system (either kafka or rabbitmq)  
Second, after 'go build':  
./mom-test (amount of iterations) (bus name) (message amount) (delay in ms before next message is send) (topic/queue name) (consumer/producer instances (up to 6)) (path to inputfile for binary data information) (encoding method (json, avro, proto))

Example RabbitMQ:  
./mom-test 1 rabbit 1000 0 guru 0 testfiles/output-1Kibi-rand-baseEnc  
./mom-test 1 rabbit 1000 2 guru 0 testfiles/output-1Kibi-rand-baseEnc avro  
./mom-test 1 rabbit 1000 10 guru 0 testfiles/output-1Kibi-rand-baseEnc proto  

Example Kafka:  
Synchronous call  
./mom-test 1 synckafka 1000 0 guru 0 testfiles/output-1Kibi-rand-baseEnc  
./mom-test 1 synckafka 1000 2 guru 0 testfiles/output-1Kibi-rand-baseEnc avro  
./mom-test 1 synckafka 1000 10 guru 0 testfiles/output-1Kibi-rand-baseEnc proto  

asynchronous call  
./mom-test 1 asynckafka 1000 0 guru 0 testfiles/output-1Kibi-rand-baseEnc  
./mom-test 1 asynckafka 1000 2 guru 0 testfiles/output-1Kibi-rand-baseEnc avro  
./mom-test 1 asynckafka 1000 10 guru 0 testfiles/output-1Kibi-rand-baseEnc proto  
