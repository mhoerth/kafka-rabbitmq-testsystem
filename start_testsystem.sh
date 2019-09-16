#!/bin/bash

/usr/local/zookeeper-3.4.12/bin/zkServer.sh start
cd /usr/local/kafka_2.11-2.2.0/bin
./kafka-server-start.sh -daemon ../config/server.properties
#/usr/local/kafka_2.11-2.2.0/bin/kafka-server-start.sh ../config/server.properties
./kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic test
echo "Testsystem sucessfully set up :-)"