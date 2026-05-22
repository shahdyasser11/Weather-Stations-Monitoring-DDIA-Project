package com.weather.centralstation;

import com.weather.WeatherMessage;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.IndexResponse;

import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.rest_client.RestClientTransport;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;

import java.util.HashMap;
import java.util.Map;

public class ElasticIndexer {

    // Elasticsearch client object
    private final ElasticsearchClient client;

    // Constructor initializes Elasticsearch connection
    public ElasticIndexer() {

        // Create  REST client for localhost:9200
        RestClient restClient = RestClient.builder(new HttpHost("localhost", 9200)).build();

        // Create transport layer using Jackson JSON mapper
        RestClientTransport transport = new RestClientTransport(restClient, new JacksonJsonpMapper());

        // Create Elasticsearch client
        client = new ElasticsearchClient(transport);
    }

    // Index weather message into Elasticsearch
    public void indexWeather(WeatherMessage msg, boolean dropped) throws Exception {

        // Create document map to store weather fields
        Map<String,Object> document = new HashMap<>();

        // Add station information
        document.put("station_id", msg.station_id);
        document.put("battery_status", msg.battery_status);

        // Add weather measurements
        document.put("humidity", msg.weather.humidity);
        document.put("temperature", msg.weather.temperature);
        document.put("wind_speed", msg.weather.wind_speed);

        // Add timestamp and dropped status
        document.put("timestamp", msg.status_timestamp);
        document.put("dropped", dropped);

        // Index document into "weather-history" index
        IndexResponse response = client.index(i -> i.index("weather-history").document(document));

        // Print indexed document ID
        System.out.println("Indexed document: " + response.id());
    }
}