package com.benife.KafakaTest;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class ProducerExample {
	
	public static void main(String[] args) throws Exception {
	Properties props = new Properties();
	props.put("metadata.broker.list", "iotdataserver1:9092");
	props.put("serializer.class", "kafka.serializer.StringEncoder");

	ProducerConfig producerConfig = new ProducerConfig(props);
	Producer<String, String> producer = new Producer<String, String>(producerConfig);
	
	
	KeyedMessage<String, String> message = new KeyedMessage<String, String>("test", "{" + 
			"    \"device_id\": \"awJo6rH\"," + 
			"    \"last_event\": {" + 
			"      \"has_sound\": false," + 
			"      \"has_motion\": true," + 
			"      \"has_person\": true," + 
			"      \"start_time\": \"2018-12-29T00:00:00.000Z\"," + 
			"      \"end_time\": \"2018-12-29T18:42:00.000Z\"" + 
			"    }" + 
			"}");
	
	/*
	List<String> supplierNames = Arrays.asList("awJo6rH", "sup2", "sup3");
	
	for(String s : supplierNames){
		KeyedMessage<String, String> message = new KeyedMessage<String, String>("test", s); 
		
		producer.send(message);
    }
    */
	
	
	producer.send(message);
	producer.close();
	}

}
