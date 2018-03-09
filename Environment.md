# IoT 빅데이터 시스템 구축

## CDH(Cloudera Hadoop EcoSystem) 설치
* CDH Version : 5.14
* JDK Version : 1.7
* Kafka Version : 0.11.0 (CDH Version 3.0.0)
* Spark Version : 2.0.0
* Spark Streaming Version : 2.0.0
* Spark Streaming Kafka Version : 0-10

## MonogoDB 
* Install Version : 3.6.3 
* Driver Version : 3.6.3 

## Flume 설정(conf)
kafka 에서 직접 HDFS로 저장 처리
<pre><code>
tier1.sources  = source1
tier1.channels = channel1
tier1.sinks = sink1

tier1.sources.source1.type = org.apache.flume.source.kafka.KafkaSource
tier1.sources.source1.zookeeperConnect = iotmainserver:2181
tier1.sources.source1.topic = test
tier1.sources.source1.groupId = flume
tier1.sources.source1.channels = channel1
tier1.sources.source1.interceptors = i1
tier1.sources.source1.interceptors.i1.type = timestamp
tier1.sources.source1.kafka.consumer.timeout.ms = 100

tier1.channels.channel1.type = memory
tier1.channels.channel1.capacity = 10000
tier1.channels.channel1.transactionCapacity = 1000

tier1.sinks.sink1.type = hdfs
tier1.sinks.sink1.hdfs.path = /tmp/kafka/%{topic}/%y-%m-%d
tier1.sinks.sink1.hdfs.rollInterval = 5
tier1.sinks.sink1.hdfs.rollSize = 0
tier1.sinks.sink1.hdfs.rollCount = 0
tier1.sinks.sink1.hdfs.fileType = DataStream
tier1.sinks.sink1.channel = channel1
</code></pre>