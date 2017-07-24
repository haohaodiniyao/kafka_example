package com.demo.kafka_demo.kafka_demo;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

public class ProducerTest {

	public static void main(String[] args) {
        Properties property = new Properties();
        property.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.157.128:9092,192.168.157.129:9092,192.168.157.130:9092");
        property.put(ProducerConfig.ACKS_CONFIG, "all");
        property.put(ProducerConfig.RETRIES_CONFIG, 1);
        property.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
        property.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
    	Producer<String,String> producer = new KafkaProducer<String,String>(property);
//    	String[] arr = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s","t","u","v","w","x","y","z"};
//    	for(String str:arr){
    	for(int i=1;i<=1000;i++){
        	producer.send(new ProducerRecord<String,String>("test-topic",i+"",i+""));	
    	}
    	for(int i=1;i<=1000;i++){
        	producer.send(new ProducerRecord<String,String>("test-topic",i+"",i+""));	
    	}
    	for(int i=1;i<=1000;i++){
        	producer.send(new ProducerRecord<String,String>("test-topic",i+"",i+""));	
    	}
    	producer.close();

	}

}
