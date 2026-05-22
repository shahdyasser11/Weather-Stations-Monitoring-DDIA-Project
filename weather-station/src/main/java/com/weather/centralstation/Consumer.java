package com.weather.centralstation;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class Consumer {

    public static void main(String[] args) {

        Properties props = new Properties();

        props.setProperty(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                "localhost:9092"
        );

        props.setProperty(
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer"
        );

        props.setProperty(
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer"
        );

        props.setProperty(
                ConsumerConfig.GROUP_ID_CONFIG,
                "weather-group"
        );

        props.setProperty(
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                "earliest"
        );

        KafkaConsumer<String,String> consumer =
                new KafkaConsumer<>(props);

        consumer.subscribe(
                Collections.singleton("weather-status")
        );

        while(true){

            ConsumerRecords<String,String> records =
                    consumer.poll(Duration.ofMillis(100));

            for(ConsumerRecord<String,String> record : records){

                System.out.println(
                        "Received: " + record.value()
                );
            }
        }
    }
}