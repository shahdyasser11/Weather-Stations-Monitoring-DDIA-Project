package com.weather.centralstation;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import com.weather.centralstation.storage.BitCaskStore;

public class CentralStation {

        public static void main(String[] args) {

                // Initialize the bitcask database
                BitCaskStore store = null;
                try {
                        store = new BitCaskStore("./bitcask_data");
                        System.out.println("BitCask Database Engine initialized.");
                } catch (IOException e) {
                        System.err.println("Failed to initialize BitCask: " + e.getMessage());
                        System.exit(1);
                }
                ParquetArchiver archiver = new ParquetArchiver("./parquet_archives");
                System.out.println("Parquet Archiver initialized.");

                // ElasticIndexer elasticIndexer = new ElasticIndexer();
                // System.out.println("Elastic search initialized");

                // start the HTTP API Server
                try {
                        HttpServer server = HttpServer.create(new InetSocketAddress("0.0.0.0", 8080), 0);

                        // Endpoint for -view-key
                        final BitCaskStore finalStore = store;
                        server.createContext("/view-key", new HttpHandler() {
                                @Override
                                public void handle(HttpExchange exchange) throws IOException {
                                        // Extract query param, e.g., /view-key?id=1
                                        String query = exchange.getRequestURI().getQuery();
                                        String stationId = query.split("=")[1];
                                        String response = finalStore.get(stationId);

                                        if (response == null)
                                                response = "Key not found.";

                                        exchange.sendResponseHeaders(200, response.getBytes().length);
                                        OutputStream os = exchange.getResponseBody();
                                        os.write(response.getBytes());
                                        os.close();
                                }
                        });

                        // Endpoint for -view-all
                        server.createContext("/view-all", new HttpHandler() {
                                @Override
                                public void handle(HttpExchange exchange) throws IOException {
                                        Map<String, String> allData = finalStore.getAll();
                                        StringBuilder csvBuilder = new StringBuilder();

                                        for (Map.Entry<String, String> entry : allData.entrySet()) {
                                                csvBuilder.append(entry.getKey()).append(",").append(entry.getValue())
                                                                .append("\n");
                                        }

                                        String response = csvBuilder.toString();
                                        exchange.sendResponseHeaders(200, response.getBytes().length);
                                        OutputStream os = exchange.getResponseBody();
                                        os.write(response.getBytes());
                                        os.close();
                                }
                        });

                        server.start();
                        System.out.println("BitCask API listening on port 8080...");

                } catch (IOException e) {
                        System.err.println("Failed to start HTTP server: " + e.getMessage());
                }

                // Kafka Consumer Setup
                ObjectMapper mapper = new ObjectMapper();
                Properties props = new Properties();
                props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka-service:9092");
                props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                                "org.apache.kafka.common.serialization.StringDeserializer");
                props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                                "org.apache.kafka.common.serialization.StringDeserializer");
                props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "weather-group");
                props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

                KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
                consumer.subscribe(Collections.singleton("weather-status"));

                System.out.println("Central Station listening for weather updates...");

                // The Infinite Stream Processing Loop
                while (true) {
                        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                        for (ConsumerRecord<String, String> record : records) {
                                String jsonValue = record.value();

                                try {
                                        JsonNode node = mapper.readTree(jsonValue);
                                        String stationId = node.get("station_id").asText();

                                        // to write to the bitcask file
                                        store.put(stationId, jsonValue);
                                        System.out.println("wrote the record to bitcask file");

                                        // to write to the parquet files
                                        archiver.bufferRecord(jsonValue);

                                } catch (Exception e) {
                                        System.err.println("Error processing record: " + e.getMessage());
                                }
                        }
                }
        }
}