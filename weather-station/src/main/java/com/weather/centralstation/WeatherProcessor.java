package com.weather.centralstation;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.weather.WeatherMessage;

public class WeatherProcessor {

    private static final String ALERT_TOPIC = "rain-alert";
    private static final int HUMIDITY_THRESHOLD = 70;

    private final KafkaProducer<String,String> producer;

    public WeatherProcessor() {

        Properties props = new Properties();

        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");

        producer = new KafkaProducer<>(props);

        System.out.println("Weather Processor Started...");
    }

    public void checkForRain(WeatherMessage weatherMessage) {

        if(weatherMessage == null) return;

        int humidity = weatherMessage.weather.humidity;

        System.out.println("Station " + weatherMessage.station_id + " humidity = " + humidity);

        if(humidity > HUMIDITY_THRESHOLD){

            String alert = "RAIN ALERT -> Station " + weatherMessage.station_id + " humidity = " + humidity;

            ProducerRecord<String,String> record = new ProducerRecord<>(ALERT_TOPIC, alert);

            producer.send(record);

            System.out.println("Rain alert published!");
            System.out.println(alert);
        }
    }

    public void close() {

        if(producer != null){

            producer.close();

            System.out.println("Weather Processor Closed");
        }
    }
}


// package com.weather.centralstation;
// import java.time.Duration;
// import java.util.Collections;
// import java.util.Properties;

// import org.apache.kafka.clients.consumer.ConsumerConfig;
// import org.apache.kafka.clients.consumer.ConsumerRecord;
// import org.apache.kafka.clients.consumer.ConsumerRecords;
// import org.apache.kafka.clients.consumer.KafkaConsumer;
// import org.apache.kafka.clients.producer.KafkaProducer;
// import org.apache.kafka.clients.producer.ProducerConfig;
// import org.apache.kafka.clients.producer.ProducerRecord;

// import com.fasterxml.jackson.databind.ObjectMapper;
// import com.weather.WeatherMessage;

// public class WeatherProcessor {

//     public static void main(String[] args)
//             throws Exception {

//         //consumer configurations
//         Properties consumerProps = new Properties();

//         consumerProps.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");

//         consumerProps.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");

//         consumerProps.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");

//         consumerProps.setProperty( ConsumerConfig.GROUP_ID_CONFIG,"weather-processors");

//         consumerProps.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");

//         KafkaConsumer<String,String> consumer =new KafkaConsumer<>(consumerProps);

//         consumer.subscribe(Collections.singleton("weather-status")); //Processor continuously listens to weather-status topic

//         // producer configurations
//         Properties producerProps = new Properties(); //for alert producer

//         producerProps.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");

//         producerProps.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");

//         producerProps.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");

//         KafkaProducer<String,String> producer = new KafkaProducer<>(producerProps);


//         ObjectMapper mapper = new ObjectMapper(); // to convert from json to weathermsg to be processed

//         System.out.println( "Weather Processor Started...");

//         // processing
//         while(true){

//             ConsumerRecords<String,String> records =consumer.poll(Duration.ofMillis(100)); //Checks Kafka every 100ms for new messages and saves it in records

//             for(ConsumerRecord<String,String> record: records){

//                 String json = record.value();
//                 WeatherMessage msg =mapper.readValue(json,WeatherMessage.class);// from json to weathermsg to process it

//                 System.out.println("Received from station: "+ msg.station_id);

//                 // rain detection
//                 if(msg.weather.humidity > 70){

//                     String alert = "RAIN ALERT -> Station " + msg.station_id
//                             + " humidity = "
//                             + msg.weather.humidity;

//                     // send alert to topic rain-alert
//                     ProducerRecord<String,String>
//                             rainRecord = new ProducerRecord<>(
//                                     "rain-alert",
//                                     alert
//                             );

//                     producer.send(rainRecord);

//                     System.out.println(
//                             "Rain alert sent!"
//                     );
//                 }
//             }
//         }
//     }
// }