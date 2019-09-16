# kafka-test

For BA

# repo description
- contains bouth kafka and rabbitmq testfiles
- [env](#env), contains the docker-compose files for the envronment
    - included are docker-compose files for starting 1 or 3 kafka / rabbitmq instances
    - special for rabbitmq is currently the helper.sh to connect the rabbitmq instnces to a cluster
- [kafka](#kafka), contains one testfile for kafka
- [rabbit](#rabbit), contains one testfile for rabbitmq
- [encoding](#encoding), contains encoding options
    - protobuf
    - avro
- [output](#output), contains the output creation (csv file)
- [structs](#structs), contains the used structs for testing (json struct and csv struct)
- [testfiles](#testfiles), contains the used testfiles (binary files) for testing
- [testing](#testing), contains a basic kafka producer and consumer
- [ToDo](#ToDo), contains a testdocument with open points and some findings during testing


# Build
go build tester.go test.pb.go

# Test execution
./tester <messages> <topicname> <ConsumerProducer ammount> <testfile (random values in one file of fixed length)>
