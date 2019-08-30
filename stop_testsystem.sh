#!/bin/bash

#/usr/local/zookeeper-3.4.12/bin/zkServer.sh start
#cd /usr/local/kafka_2.11-2.2.0/bin
#./kafka-server-start.sh -daemon ../config/server.properties
#/usr/local/kafka_2.11-2.2.0/bin/kafka-server-start.sh ../config/server.properties
#./kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic TestSegment

#read idle

#if [ "$idle" = "stop" ]
#then
cd /usr/local/kafka_2.11-2.2.0/bin
./kafka-topics.sh --zookeeper localhost:2181 --delete --topic test
./kafka-server-stop.sh
rm -r C\:Kafka-umgebungkafka-logs_wsl-4/
cd ~
/usr/local/zookeeper-3.4.12/bin/zkServer.sh stop
cd /usr/local/kafka_2.11-2.2.0/config
rm -r /tmp/zookeeper
echo "Kafka and Zookeeper sucessfully shut down and logs cleared"
#fi