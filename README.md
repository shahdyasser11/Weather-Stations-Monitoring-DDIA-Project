# Weather-Stations-Monitoring-DDIA-Project

## Project Overview

This project simulates a distributed weather monitoring system using Apache Kafka.

The system contains:

- Weather Stations (Kafka Producers)
- Central Station (Kafka Consumer)
- Weather Processor (Rain Trigger Detection)
- Kafka Topics for streaming and alerts

The processor analyzes incoming weather readings and publishes rain alerts when humidity exceeds 70%.

---

# Technologies Used

- Java
- Apache Kafka
- Docker
- Maven
- Jackson JSON Library

---

# Docker Containers Used

## Kafka Broker Container

Container Name:

```bash
broker
```

Docker Image:

```bash
apache/kafka:latest
```

Run Command:

```bash
docker run -d \
  --name broker \
  -p 9092:9092 \
  apache/kafka:latest
```

---

# Kafka Topics

## 1) weather-status

Main topic used by weather stations to publish weather readings.

Create Topic:

```bash
./kafka-topics.sh \
--create \
--topic weather-status \
--bootstrap-server localhost:9092
```

---

## 2) rain-alert

Topic used by the processor to publish rain alerts.

Create Topic:

```bash
./kafka-topics.sh \
--create \
--topic rain-alert \
--bootstrap-server localhost:9092
```

---

# Verify Existing Topics

```bash
./kafka-topics.sh \
--list \
--bootstrap-server localhost:9092
```

---

# Enter Kafka Container

```bash
docker exec --workdir /opt/kafka/bin/ -it broker sh
```

---

# Kafka Console Consumer Commands

## Read weather-status Topic

```bash
./kafka-console-consumer.sh \
--bootstrap-server localhost:9092 \
--topic weather-status \
--from-beginning
```

---

## Read rain-alert Topic

```bash
./kafka-console-consumer.sh \
--bootstrap-server localhost:9092 \
--topic rain-alert \
--from-beginning
```

---

# Implemented Steps

## 1) Kafka Setup

- Installed Docker
- Pulled Apache Kafka Docker image
- Started Kafka broker container
- Verified Kafka broker is running

---

## 2) Kafka Producer Test

Implemented a simple Kafka producer to verify Kafka connectivity.

Test message:

```text
Hey Kafka!
```

---

## 3) Weather Station Producer

Implemented a weather station simulator that:

- Generates random weather readings
- Generates battery status
- Simulates dropped messages
- Serializes weather data into JSON
- Publishes messages to Kafka topic `weather-status`

---

## 4) Kafka Consumer / Central Station

Implemented a central station service that:

- Subscribes to `weather-status`
- Consumes weather messages
- Deserializes JSON messages into Java objects
- Passes messages to the processor

---

## 5) Weather Processor

Implemented a weather processor that:

- Receives weather readings
- Detects rain conditions
- Publishes alerts to `rain-alert`

Rain detection rule:

```text
Humidity > 70%
```

---

# System Architecture

```text
Weather Station
       ↓
weather-status topic
       ↓
CentralStation
       ↓
WeatherProcessor
       ↓
rain-alert topic
```

---

# Project File Structure

```text
Weather-Stations-Monitoring-DDIA-Project/
│
├── weather-station/
│   │
│   ├── pom.xml
│   │
│   └── src/main/java/com/weather/
|   ├── Weather.java
│   ├── WeatherMessage.java
|   ├── stations/
│   |   │
│   |   ├── WeatherStation.java
│   |   └── TestProducer.java
|   |
|   ├── centralstation/
│       │
│       ├── WeatherProcessor.java
│       └── centralStation.java
```

---

# JSON Weather Message Example

```json
{
  "station_id": 1,
  "s_no": 15,
  "battery_status": "medium",
  "status_timestamp": 1716322000,
  "weather": {
    "humidity": 82,
    "temperature": 23,
    "wind_speed": 44
  }
}
```

---

# Rain Alert Example

```text
RAIN ALERT -> Station 1 humidity = 82
```

---

# Maven Dependencies Used

## Kafka Clients

```xml
<dependency>
    <groupId>org.apache.kafka</groupId>
    <artifactId>kafka-clients</artifactId>
    <version>3.7.0</version>
</dependency>
```

---

## Jackson Databind

```xml
<dependency>
    <groupId>com.fasterxml.jackson.core</groupId>
    <artifactId>jackson-databind</artifactId>
    <version>2.17.0</version>
</dependency>
```

---

# Build Project


```bash
cd weather-station
mvn clean package
```

---

# Run Weather Station

```bash
java -lesaa
```

---

# Run Central Station

```bash
java lesaa
```






# ElasticSearch & Kibana Setup

## Pull ElasticSearch Image

```bash
docker pull elasticsearch:8.13.4
```

---

## Run ElasticSearch Container

```bash
docker run -d \
  --name elasticsearch \
  -p 9200:9200 \
  -e "discovery.type=single-node" \
  -e ES_JAVA_OPTS="-Xms512m -Xmx512m" \
  -e "xpack.security.enabled=false" \
  elasticsearch:8.13.4
```

The following JVM configuration reduces memory consumption for Virtual Machines:

```text
ES_JAVA_OPTS="-Xms512m -Xmx512m"
```

---

## Verify ElasticSearch

Open browser:

```text
http://localhost:9200
```

A JSON response confirms that ElasticSearch is running correctly.

---

## Pull Kibana Image

```bash
docker pull kibana:8.13.4
```

---

## Run Kibana Container

```bash
docker run -d \
  --name kibana \
  -p 5601:5601 \
  --link elasticsearch:elasticsearch \
  -e ELASTICSEARCH_HOSTS=http://elasticsearch:9200 \
  kibana:8.13.4
```

---

## Access Kibana Dashboard

Open browser:

```text
http://localhost:5601
```

---

# Kibana Analytics

The Kibana dashboards visualize:

- humidity trends
- temperature trends
- wind speed analytics
- low / medium / high battery status statistics
- dropped message statistics
- station activity monitoring

---

## Battery Status Statistics Dashboard

The following dashboard visualizes the distribution of:

- low battery
- medium battery
- high battery

using Kibana vertical chart analytics.

![Battery Dashboard](imgs/status.png)

---

## Dropped Messages Dashboard

The following dashboard visualizes:

- dropped messages
- successfully delivered messages

to monitor communication reliability between weather stations and the central station.

![dropped messages](imgs/dropped.png)


---

# Build Weather Station Image

Inside `weather-station/`:

```bash
docker build -f Dockerfile.weather-stations -t weather-station .
```

---

# Build Central Station Image

Inside `weather-station/`:

```bash
docker build -f Dockerfile.central-stations -t central-station .
```

---

# Run Central Station Container

```bash
docker run --network host --name central central-station
```

---

# Run Weather Station Containers

## Station 1

```bash
docker run --network host weather-station 1
```

---

## Station 2

```bash
docker run --network host weather-station 2
```

---

## Station 3

```bash
docker run --network host weather-station 3
```

---