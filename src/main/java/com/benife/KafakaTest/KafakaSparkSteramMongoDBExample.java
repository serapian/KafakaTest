package com.benife.KafakaTest;

import java.io.IOException;
import java.util.*;
import org.apache.spark.SparkConf;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka010.*;
import org.bson.Document;
import com.mongodb.DBObject;
import com.mongodb.util.JSON;

import com.benife.KafakaTest.DeviceA.Last_event;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import kafka.serializer.StringDecoder;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import scala.Tuple2;

import org.apache.spark.sql.SparkSession;


public class KafakaSparkSteramMongoDBExample  implements java.io.Serializable {

	private static MongoClient mongoClient;

	public static void main(String[] args) {

		Map<String, Object> kafkaParams = new HashMap<>();
		kafkaParams.put("bootstrap.servers", "iotdataserver1:9092,iotdataserver2:9092,iotmainserver:9092");
		kafkaParams.put("key.deserializer", StringDeserializer.class);
		kafkaParams.put("value.deserializer", StringDeserializer.class);
		kafkaParams.put("group.id", "use_a_separate_group_id_for_each_stream");
		kafkaParams.put("auto.offset.reset", "latest");
		kafkaParams.put("enable.auto.commit", false);
		
		Collection<String> topics = Arrays.asList("test");
		

	    // Create context with a 2 seconds batch interval
	    SparkConf sparkConf = new SparkConf().setMaster("local[2]").setAppName("KafakaSparkSteramExample");
	    	    
	    JavaStreamingContext streamingContext = new JavaStreamingContext(sparkConf, Durations.seconds(2));	    

		JavaInputDStream<ConsumerRecord<String, String>> stream =
		  KafkaUtils.createDirectStream(
		    streamingContext,
		    LocationStrategies.PreferConsistent(),
		    ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams)
		  );
		  
	   	    
		//stream.mapToPair(record -> new Tuple2<>(record.key(), record.value()));
		
		 // Read value of each message from Kafka and return it
		
		final String rowData = "";
		
        
        JavaDStream<String> lines = stream.map(new Function<ConsumerRecord<String,String>, String>() {
            @Override
            public String call(ConsumerRecord<String, String> kafkaRecord) throws Exception {
            	System.out.println("Line:" +kafkaRecord.value());
                return kafkaRecord.value();
            }
        });
        
		
        
        JavaDStream<String> filter_sample = lines.filter(new Function<String, Boolean>() {
            private static final long serialVersionUID = 1L;
            @Override
            public Boolean call(String dump) throws Exception {

            	ObjectMapper mapper = new ObjectMapper();
            	Device device = mapper.readValue(dump, Device.class);
            	
                if(device.getDevice_id().equals("awJo6rH")){
            		System.out.println("contains!!!");
                        return true;
                }
                else{
                        return false;
                }
            }
        });
        

       
    	
      
        filter_sample.foreachRDD(new VoidFunction<JavaRDD<String>>() {
            @Override
            public void call(JavaRDD<String> data) throws Exception {           	
            	data.foreach(new VoidFunction<String>() {
					@Override
                     public void call(String s) throws Exception {
                    	 System.out.println("!!!!final Data:" + s);
                    	 
                     	//Document doc = new Document("name", "test")
                          //      .append("data", "aaaaa");
                              //documentMongoCollection.insertOne(doc);
                    	 storeData(s);
                     }
                 });
            	System.out.println("AAA:" + data.toString());
            	
            }
        });
    
        
        /*
        
        // Break every message into words and return list of words
        JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String line) throws Exception {
                return Arrays.asList(line.split(" ")).iterator();
            }
        });

        // Take every word and return Tuple with (word,1)
        JavaPairDStream<String,Integer> wordMap = words.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String word) throws Exception {
                return new Tuple2<>(word,1);
            }
        });

        // Count occurance of each word
        JavaPairDStream<String,Integer> wordCount = wordMap.reduceByKey(new Function2<Integer, Integer, Integer>() {
             @Override
             public Integer call(Integer first, Integer second) throws Exception {
                 return first+second;
             }
         });
        
        

        
        //Print the word count
        wordCount.print();
        */
        //wordCount.foreachRDD(foreachFunc);

        streamingContext.start();
        try {
			streamingContext.awaitTermination();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
	
	
	private static void storeData(String paramT) {
		
		if(mongoClient == null)
			mongoClient = new MongoClient("localhost");
		
		MongoDatabase db = mongoClient.getDatabase("iot");
	    MongoCollection<Document> documentMongoCollection = db.getCollection("sensor");
	    
	    ObjectMapper mapper = new ObjectMapper();
    	Device device;
		try {
			device = mapper.readValue(paramT, Device.class);
			
	    	Document doc = new Document("device_id", device.getDevice_id())
	                .append("last_event", 
	                		new Document("has_sound", device.getLast_event().getHas_sound())
	                		.append("has_motion", device.getLast_event().getHas_motion())
	                		.append("has_person", device.getLast_event().getHas_person())
	                		.append("start_time", device.getLast_event().getStart_time())
	                		.append("end_time", device.getLast_event().getEnd_time()));
	    	
	    	documentMongoCollection.insertOne(doc);
	    	
		} catch (JsonParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (JsonMappingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	

		
	}

}

class Device
{
private String device_id;
	
	public static class Last_event {
		private boolean has_sound;
		private boolean has_motion;
		private boolean has_person;
		private String start_time;
		private String end_time;
		
		public boolean getHas_sound()
		{
			return has_sound;
		}
		
		public boolean getHas_motion()
		{
			return has_motion;
		}
		
		public boolean getHas_person()
		{
			return has_person;
		}
		
		public String getStart_time()
		{
			return start_time;
		}
		
		public String getEnd_time()
		{
			return end_time;
		}
		
		public void setHas_sound(boolean b)
		{
			has_sound = b;
		}
		
		public void setHas_motion(boolean b)
		{
			has_motion = b;
		}
		
		public void setHas_person(boolean b)
		{
			has_person= b;
		}
		
		public void setStart_time(String t)
		{
			start_time = t;
		}
		
		public void setEnd_time(String t)
		{
			end_time = t;
		}
	}
	
	public String getDevice_id()
	{
		return device_id;
	}
	
	public void setDevice_id(String t)
	{
		device_id = t;
	}
	
	private Last_event last_event;
	
	public Last_event getLast_event()
	{
		return last_event;
	}
	
	public void setLast_event(Last_event t)
	{
		last_event = t;
	}
}
