error id: file://<WORKSPACE>/weather-station/src/main/java/com/weather/centralstation/CentralStation.java
file://<WORKSPACE>/weather-station/src/main/java/com/weather/centralstation/CentralStation.java
### com.thoughtworks.qdox.parser.ParseException: syntax error @[21,1]

error in qdox parser
file content:
```java
offset: 675
uri: file://<WORKSPACE>/weather-station/src/main/java/com/weather/centralstation/CentralStation.java
text:
```scala
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
import com.weather.centralstation.storage.BitCaskStore;
import com.weather.centralstation.ParquetArchiver

i@@mport com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpExchange;

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
                archiver = new com.weather.centralstation.archiver.ParquetArchiver("./parquet_archives");
                System.out.println("Parquet Archiver initialized.");

                // start the HTTP API Server
                try {
                        HttpServer server = HttpServer.create(new InetSocketAddress(8080), 0);

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
                props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
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

                                        store.put(stationId, jsonValue);
                                        // System.out.println("Stored update for Station " + stationId);

                                } catch (Exception e) {
                                        System.err.println("Error processing record: " + e.getMessage());
                                }
                        }
                }
        }
}
```

```



#### Error stacktrace:

```
com.thoughtworks.qdox.parser.impl.Parser.yyerror(Parser.java:2025)
	com.thoughtworks.qdox.parser.impl.Parser.yyparse(Parser.java:2147)
	com.thoughtworks.qdox.parser.impl.Parser.parse(Parser.java:2006)
	com.thoughtworks.qdox.library.SourceLibrary.parse(SourceLibrary.java:232)
	com.thoughtworks.qdox.library.SourceLibrary.parse(SourceLibrary.java:190)
	com.thoughtworks.qdox.library.SourceLibrary.addSource(SourceLibrary.java:94)
	com.thoughtworks.qdox.library.SourceLibrary.addSource(SourceLibrary.java:89)
	com.thoughtworks.qdox.library.SortedClassLibraryBuilder.addSource(SortedClassLibraryBuilder.java:162)
	com.thoughtworks.qdox.JavaProjectBuilder.addSource(JavaProjectBuilder.java:174)
	scala.meta.internal.mtags.JavaMtags.indexRoot(JavaMtags.scala:49)
	scala.meta.internal.metals.SemanticdbDefinition$.foreachWithReturnMtags(SemanticdbDefinition.scala:99)
	scala.meta.internal.metals.Indexer.indexSourceFile(Indexer.scala:560)
	scala.meta.internal.metals.Indexer.$anonfun$reindexWorkspaceSources$3(Indexer.scala:691)
	scala.meta.internal.metals.Indexer.$anonfun$reindexWorkspaceSources$3$adapted(Indexer.scala:688)
	scala.collection.IterableOnceOps.foreach(IterableOnce.scala:630)
	scala.collection.IterableOnceOps.foreach$(IterableOnce.scala:628)
	scala.collection.AbstractIterator.foreach(Iterator.scala:1313)
	scala.meta.internal.metals.Indexer.reindexWorkspaceSources(Indexer.scala:688)
	scala.meta.internal.metals.MetalsLspService.$anonfun$onChange$2(MetalsLspService.scala:940)
	scala.runtime.java8.JFunction0$mcV$sp.apply(JFunction0$mcV$sp.scala:18)
	scala.concurrent.Future$.$anonfun$apply$1(Future.scala:691)
	scala.concurrent.impl.Promise$Transformation.run(Promise.scala:500)
	java.base/java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1136)
	java.base/java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:635)
	java.base/java.lang.Thread.run(Thread.java:840)
```
#### Short summary: 

QDox parse error in file://<WORKSPACE>/weather-station/src/main/java/com/weather/centralstation/CentralStation.java