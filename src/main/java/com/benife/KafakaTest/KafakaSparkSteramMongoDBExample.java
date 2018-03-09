package com.benife.KafakaTest;

/*
 * Kafka로 데이터 받아서 실시간 이벤트 처리(Spark Streaming) 예제
 * 특정 ID인것만 저장
 */

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
import com.benife.KafakaTest.DeviceA.Data;
import com.benife.KafakaTest.DeviceA.Header;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

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
            	
                if(device.getHeader().getCommand().equals("abStatus")){
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
						
	    	Document doc = new Document();
	    	doc.append("header",new Document("command", device.getHeader().getCommand())
		                		.append("sourceDeviceId", device.getHeader().getSourceDeviceId())
		                		.append("has_person", device.getHeader().getTargetDeviceId()));
	        doc.append("data",new Document("errorCode", device.getData().getErrorCode()));
	    	
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
	public static class Header {
		private String command;
		private String sourceDeviceId;
		private String targetDeviceId;
		
		public String getCommand()
		{
			return command;
		}
		
		public String getSourceDeviceId()
		{
			return sourceDeviceId;
		}
		
		public String getTargetDeviceId()
		{
			return targetDeviceId;
		}
		
		public void setCommand(String s)
		{
			command = s;
		}
		
		public void setSourceDeviceId(String s)
		{
			sourceDeviceId = s;
		}
		
		public void setTargetDeviceId(String s)
		{
			targetDeviceId= s;
		}
	}
	
	public static class Data {
		private String errorCode;
		
		public String getErrorCode()
		{
			return errorCode;
		}
		
				
		public void setErrorCode(String s)
		{
			errorCode = s;
		}
	}
		
	private Header header;
	private Data data;
	
	public Header getHeader()
	{
		return header;
	}
	
	public void setHeader(Header h)
	{
		header = h;
	}
	
	public Data getData()
	{
		return data;
	}
	
	public void setData(Data d)
	{
		data = d;
	}
}
