package com.github.yusufbestas.kafka.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;


public class ConsumerDemo {

    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(ConsumerDemo.class);
        String bootstrapServers="127.0.0.1:9092";
        String groupId="my-fourth-app";
        String topic="first_topic";

        //create consumer configs
        Properties properties=new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");

        // create consumer
        KafkaConsumer<String,String> consumer= new KafkaConsumer(properties);

        //Subscribe consumer to our topic(s)
        consumer.subscribe(List.of(topic));

        //poll for new data
        while(true){
            ConsumerRecords<String,String> records=
                    consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> records1:records){

            }
        }
    }

}
