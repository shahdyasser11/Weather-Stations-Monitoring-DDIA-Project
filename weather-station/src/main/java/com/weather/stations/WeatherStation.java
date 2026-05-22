package com.weather.stations;
import java.util.Properties;
import java.util.Random;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.fasterxml.jackson.databind.ObjectMapper; //convert Java object to JSON.
import com.weather.Weather;
import com.weather.WeatherMessage;

public class WeatherStation {

    public static void main(String[] args) throws Exception {

        Properties props = new Properties(); //creates configuration object.

        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092"); //defines kafka place

        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer"); // convert the key which is a string to bytes, because kafka sends bytes

        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer"); // convert the msg body which is a string to bytes, because kafka sends bytes


        KafkaProducer<String, String> producer = new KafkaProducer<>(props); // creates the producer

        ObjectMapper mapper = new ObjectMapper();

        Random random = new Random();

        long serial = 1; //message number

        while(true){

            WeatherMessage msg = new WeatherMessage();

            msg.station_id = Long.parseLong(args[0]);
            msg.s_no = serial++;

            int batteryRand = random.nextInt(100);

            if(batteryRand < 30)
                msg.battery_status = "low";
            else if(batteryRand < 70)
                msg.battery_status = "medium";
            else
                msg.battery_status = "high";

            msg.status_timestamp = System.currentTimeMillis();

            msg.weather = new Weather();

            msg.weather.humidity = random.nextInt(100);

            msg.weather.temperature = random.nextInt(40);

            msg.weather.wind_speed = random.nextInt(100);

            // 10% dropped messages
            if(random.nextInt(100) < 10){

                System.out.println("Message Dropped");

            } else {

                String jsonMsg = mapper.writeValueAsString(msg); //transforms Java object into JSON string

                ProducerRecord<String,String> record = new ProducerRecord<>("weather-status",jsonMsg); //produces kafka record (takes the msg to a certain topic)
                producer.send(record); //transmits message to Kafka broker

                System.out.println("Sent: " + jsonMsg);
            }

            Thread.sleep(1000);
        }
    }
}