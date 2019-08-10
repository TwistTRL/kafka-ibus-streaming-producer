# kafka-tcp-server-producer
This is a script that opens a TCP server listening for a single incoming connection and write data to Kafka cluster.
At the same time operational logs are stored onto Kafka.

## Installation
```
pip install git+https://github.com/TwistTRL/kafka-tcp-server-producer
```

## Usage:
```
kafka-tcp-server-producer.py <kafkaHost> <kafkaPort> <tcpHost> <tcpPort> <groupId> <topic> <logTopic> <interval>
```
