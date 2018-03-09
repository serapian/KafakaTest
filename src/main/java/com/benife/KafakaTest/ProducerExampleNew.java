package com.benife.KafakaTest;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

public class ProducerExampleNew {
	
	public static void main(String[] args) throws Exception {
	
	Properties configProperties = new Properties();
	configProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"iotdataserver1:9092");
	configProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.ByteArraySerializer");
	configProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
	 
	org.apache.kafka.clients.producer.Producer producer = new KafkaProducer(configProperties);
	
	String testJSON = "{" 
			+ "\"header\" : {" 
	        + "\"command\" : \"abStatus\","
	        + "\"sourceDeviceId\" : \"P_22340\","
	        + "\"targetDeviceId\" : \"G_12345\""
	        + "},"
	        + "\"data\" : {"
	        + "\"errorCode\" : 101"
	        + "}"
			+ "}";
	
	 ProducerRecord<String, String> rec = new ProducerRecord<String, String>("test", testJSON);
	 producer.send(rec);
	 producer.close();
	}

}
