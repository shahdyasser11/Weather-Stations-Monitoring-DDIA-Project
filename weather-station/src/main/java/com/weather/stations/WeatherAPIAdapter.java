package com.weather.stations;

import java.util.Properties;

import org.apache.hc.client5.http.fluent.Request;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.weather.Weather;
import com.weather.WeatherMessage;

public class WeatherAPIAdapter {

    public static void main(String[] args) throws Exception {

        // kafka producer configuration
        Properties props = new Properties();

        // kafka broker address
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        // serializer for kafka message key
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        // serializer for kafka message value
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        // kafka producer object used to send messages to kafka
        KafkaProducer<String,String> producer = new KafkaProducer<>(props);

        // jackson object for json parsing and serialization
        ObjectMapper mapper = new ObjectMapper();

        // sequential message number
        long serial = 1;

        // infinite loop continuously fetching weather data
        while(true){

            // open-meteo api url for alexandria weather
            String url = "https://api.open-meteo.com/v1/forecast"
                    + "?latitude=31.2001"
                    + "&longitude=29.9187"
                    + "&current=temperature_2m,relative_humidity_2m,wind_speed_10m";

            // send http get request to open-meteo api and receive response as string
            String response = Request.get(url).execute().returnContent().asString();

            // parse response json into json tree
            JsonNode root = mapper.readTree(response);

            // extract current weather object
            JsonNode current = root.get("current");

            // create internal weather message object
            WeatherMessage msg = new WeatherMessage();

            // station id reserved for external api source
            msg.station_id = 111;

            // sequential message id
            msg.s_no = serial++;

            // battery not applicable for external api
            msg.battery_status = "external-api";

            // current system timestamp
            msg.status_timestamp = System.currentTimeMillis();

            // api data is not dropped
            msg.dropped = false;

            // create nested weather object
            msg.weather = new Weather();

            // extract temperature from api response
            msg.weather.temperature =
                    (int) current.get("temperature_2m").asDouble();

            // extract humidity from api response
            msg.weather.humidity =
                    current.get("relative_humidity_2m").asInt();

            // extract wind speed from api response
            msg.weather.wind_speed =
                    (int) current.get("wind_speed_10m").asDouble();

            // convert java object into json string
            String json = mapper.writeValueAsString(msg);

            // kafka message object
            ProducerRecord<String,String> record =
                    new ProducerRecord<>("weather-status", json);

            // send message to kafka topic
            producer.send(record);

            // print sent message
            System.out.println("Open-Meteo Sent: " + json);

            // wait 30 seconds before next api request
            Thread.sleep(30000);
        }
    }
}