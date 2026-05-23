package com.weather.centralstation;

import java.util.HashMap;
import java.util.Map;

import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;

public class ParquetToElasticImporter {

    // object responsible for indexing documents into elasticsearch
    private final ElasticIndexer elasticIndexer;

    // constructor initializes elasticsearch connection
    public ParquetToElasticImporter() {

        elasticIndexer = new ElasticIndexer();
    }

    // reads parquet file and indexes all records into elasticsearch
    public void importParquet(String parquetFile) throws Exception {

        // create parquet reader object
        ParquetReader<GenericRecord> reader =
                AvroParquetReader.<GenericRecord>builder(new Path(parquetFile))
                        .withConf(new Configuration())
                        .build();

        GenericRecord record;

        // read parquet records one by one
        while((record = reader.read()) != null){

            // map represents elasticsearch json document
            Map<String,Object> document = new HashMap<>();

            // extract parquet columns and store in elasticsearch document
            document.put("station_id", Long.parseLong(record.get("station_id").toString()) );

            document.put("battery_status", record.get("battery_status").toString());

            document.put("humidity", record.get("humidity"));

            document.put("temperature", record.get("temperature"));

            document.put("wind_speed", record.get("wind_speed"));

            document.put("timestamp", record.get("status_timestamp"));
            document.put("dropped", record.get("dropped"));

            // send document to elasticsearch index
            elasticIndexer.indexDocument(document);
        }

        // close parquet reader
        reader.close();

        System.out.println("Parquet imported into ElasticSearch");
    }
}