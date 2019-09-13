# kafka-test

For BA

# repo description
- contains bouth kafka and rabbitmq testfiles
- link(env contains the docker-compose files for the envronment)
    - included are dockerfiles for starting 1 or 3 kafka / rabbitmq instances
link (producer+consumer contains complete testfiles for kafka / rabbitmq)
    - folder kafka conatains the kafka testfile
    - folder rabbitmq contains the rabbitmq testfile

# Build
go build tester.go test.pb.go

# Test execution
./tester <messages> <topicname> <ConsumerProducer ammount> <testfile (random values in one file of fixed length)>
