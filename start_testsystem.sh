#!/bin/bash

/usr/local/zookeeper-3.4.12/bin/zkServer.sh start
cd /usr/local/kafka_2.11-2.2.0/bin
./kafka-server-start.sh -daemon ../config/server.properties
#/usr/local/kafka_2.11-2.2.0/bin/kafka-server-start.sh ../config/server.properties
./kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic test
echo "Testsystem sucessfully set up :-)"
#read idle

#if [ "$idle" = "stop" ]
#then
#./kafka-topics.sh --zookeeper localhost:2181 --delete --topic TestSegment
#./kafka-server-stop.sh
#rm -r C\:Kafka-umgebungkafka-logs_wsl-4/
#cd ~
#/usr/local/zookeeper-3.4.12/bin/zkServer.sh stop
#fi