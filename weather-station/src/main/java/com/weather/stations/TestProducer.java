package com.weather.stations;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

public class TestProducer {

    public static void main(String[] args) {

        Properties properties = new Properties();

        properties.setProperty(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                "127.0.0.1:9092"
        );

        properties.setProperty(
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer"
        );

        properties.setProperty(
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer"
        );

        try(KafkaProducer<String,String> producer =
                    new KafkaProducer<>(properties)) {

            ProducerRecord<String,String> record =
                    new ProducerRecord<>(
                            "my_first",
                            "Hey Kafka!"
                    );

            producer.send(record);

            System.out.println(
                    "Message Sent!"
            );
        }
    }
}