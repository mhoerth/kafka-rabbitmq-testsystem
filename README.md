# Kafka + RabbitMQ testsystem

This repository contains a testsystem for messagebus systems (currently Apache kafka and RabbitMQ), which is used in my batchelor thesis.

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
- [ToDo](#ToDo), contains a testdocument with open points and some findings during testing
- the files 'start_testsystem.sh' and 'stop_testsystem.sh' are a method to start/configure a local testsystem (requires a running local instance of Kafka and Zookeeper)

# Requirements
On the basis of a golang image this testsystem needs a few other components to interact with the different bus syxstems and to use different encoding formats
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
./kafka-test-repo (bus name) (message amount) (topic/queue name) (consumer/producer instnces (up to 6)) (path to inputfile for binary data information) (encoding method (json, avro, proto))
Example RabbitMQ:
./kafka-test-repo rabbit 1000 guru 0 testfiles/output-1Kibi-rand
./kafka-test-repo rabbit 1000 guru 0 testfiles/output-1Kibi-rand avro
./kafka-test-repo rabbit 1000 guru 0 testfiles/output-1Kibi-rand proto

Example Kafka:
./kafka-test-repo kafka 1000 guru 0 testfiles/output-1Kibi-rand
./kafka-test-repo kafka 1000 guru 0 testfiles/output-1Kibi-rand avro
./kafka-test-repo kafka 1000 guru 0 testfiles/output-1Kibi-rand proto