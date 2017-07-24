package com.demo.kafka_demo.kafka_demo;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class ConsumerTest {

	public static void main(String[] args) {
		Properties property = new Properties();
		property.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.157.128:9092,192.168.157.129:9092,192.168.157.130:9092");
		property.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group");
		property.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
		property.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
		Consumer<String,String> consumer = new KafkaConsumer<String,String>(property);
		consumer.subscribe(Arrays.asList("test-topic","test-topic2"));
		while(true){
			ConsumerRecords<String,String> records = consumer.poll(100);
			for(ConsumerRecord<String,String> record:records){
				System.out.println("offset:"+record.offset()+",key:"+record.key()+",value:"+record.value());
			}
		}
	}

}
