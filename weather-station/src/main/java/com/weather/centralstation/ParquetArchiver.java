package com.weather.centralstation;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class ParquetArchiver {
    private final int BATCH_SIZE = 100;
    private final String archiveDir;
    private final List<JsonNode> recordBuffer;
    private final ObjectMapper mapper;
    private final Schema avroSchema;

    public ParquetArchiver(String archiveDir) {
        this.archiveDir = archiveDir;
        this.recordBuffer = new ArrayList<>(BATCH_SIZE);
        this.mapper = new ObjectMapper();

        // Define the schema based on the JSON structure for Elasticsearch
        String schemaString = "{"
                + "\"type\": \"record\","
                + "\"name\": \"WeatherRecord\","
                + "\"fields\": ["
                + "  {\"name\": \"station_id\", \"type\": \"long\"},"
                + "  {\"name\": \"s_no\", \"type\": \"long\"},"
                + "  {\"name\": \"battery_status\", \"type\": \"string\"},"
                + "  {\"name\": \"status_timestamp\", \"type\": \"long\"},"
                + "  {\"name\": \"humidity\", \"type\": \"int\"},"
                + "  {\"name\": \"temperature\", \"type\": \"int\"},"
                + "  {\"name\": \"wind_speed\", \"type\": \"int\"},"
                + "  {\"name\": \"dropped\", \"type\": \"boolean\"}"
                + "]"
                + "}";
        this.avroSchema = new Schema.Parser().parse(schemaString);
    }

    // Add incoming Kafka records to the memory buffer
    public synchronized void bufferRecord(String jsonValue) {
        try {
            JsonNode node = mapper.readTree(jsonValue);
            recordBuffer.add(node);

            if (recordBuffer.size() >= BATCH_SIZE) {
                flushToDisk();
            }
        } catch (Exception e) {
            System.err.println("Failed to parse JSON for archiving: " + e.getMessage());
        }
    }

    // Write the batch to Parquet and partition by Time and Station ID
    private synchronized void flushToDisk() {
        if (recordBuffer.isEmpty())
            return;

        System.out.println(
                "[ParquetArchiver] Batch size reached. Flushing " + recordBuffer.size() + " records to disk...");

        // Group records by Station ID to partition them
        Map<String, List<JsonNode>> partitionedData = new HashMap<>();
        for (JsonNode node : recordBuffer) {
            String stationId = node.get("station_id").asText();
            partitionedData.computeIfAbsent(stationId, k -> new ArrayList<>()).add(node);
        }

        String currentDate = new SimpleDateFormat("yyyy-MM-dd").format(new Date());

        for (Map.Entry<String, List<JsonNode>> entry : partitionedData.entrySet()) {
            long stationId = Long.parseLong(entry.getKey());
            List<JsonNode> stationRecords = entry.getValue();

            // Create partition directories: archiveDir/date/station_id/
            String partitionPath = archiveDir + "/date=" + currentDate + "/station=" + stationId;
            new File(partitionPath).mkdirs();

            String filePath = partitionPath + "/" + System.currentTimeMillis() + ".parquet";
            Path hadoopPath = new Path(filePath);

            try (ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(hadoopPath)
                    .withSchema(avroSchema)
                    .withCompressionCodec(CompressionCodecName.SNAPPY)
                    .withConf(new Configuration())
                    .build()) {

                for (JsonNode node : stationRecords) {
                    GenericRecord record = new GenericData.Record(avroSchema);
                    record.put("station_id", stationId);
                    record.put("s_no", node.get("s_no").asLong());
                    record.put("battery_status", node.get("battery_status").asText());
                    record.put("status_timestamp", node.get("status_timestamp").asLong());
                    record.put("humidity", node.get("weather").get("humidity").asInt());
                    record.put("temperature", node.get("weather").get("temperature").asInt());
                    record.put("wind_speed", node.get("weather").get("wind_speed").asInt());
                    record.put("dropped", node.get("dropped").asBoolean());


                    writer.write(record);
                }
            } catch (IOException e) {
                System.err.println("Failed to write Parquet file: " + e.getMessage());
            }
            // import parquet file into elasticsearch
            try {

                ParquetToElasticImporter importer =
                        new ParquetToElasticImporter();

                importer.importParquet(filePath);

            } catch (Exception e) {

                System.err.println(
                        "Failed to import parquet into elasticsearch: "
                        + e.getMessage()
                );
            }
        }



        // Clear the buffer for the next 10,000 records
        recordBuffer.clear();
        System.out.println("[ParquetArchiver] Flush complete.");
    }

    
}