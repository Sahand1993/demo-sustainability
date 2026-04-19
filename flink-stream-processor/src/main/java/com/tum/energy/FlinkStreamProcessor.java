package com.tum.energy;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.configuration.Configuration;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.table.api.Expressions.$;

public class FlinkStreamProcessor {
    
    // Configuration
    private static final String SCHEMA_PATH = System.getProperty("user.home") + "/demo-video/schemas";
    private static final String WAREHOUSE_PATH = System.getProperty("user.home") + "/demo-video/iceberg-warehouse/";
    private static final String CHECKPOINT_PATH = System.getProperty("user.home") + "/demo-video/checkpoints/flink/";
    private static final String ICEBERG_NAMESPACE = "tpch_kafka";
    
    /**
     * Auto-discover topics from schema files
     */
    private static List<String> discoverTopics() {
        List<String> topics = new ArrayList<>();
        File schemaDir = new File(SCHEMA_PATH);
        
        if (schemaDir.exists() && schemaDir.isDirectory()) {
            File[] schemaFiles = schemaDir.listFiles((dir, name) -> name.endsWith(".json"));
            if (schemaFiles != null) {
                for (File file : schemaFiles) {
                    String tableName = file.getName().replace(".json", "");
                    topics.add(tableName);
                }
            }
        }
        
        return topics;
    }
    
    /**
     * Load column names from schema file
     */
    private static List<String> loadSchema(String topic) throws Exception {
        String schemaFile = SCHEMA_PATH + "/" + topic + ".json";
        String content = new String(Files.readAllBytes(Paths.get(schemaFile)));
        ObjectMapper mapper = new ObjectMapper();
        JsonNode schema = mapper.readTree(content);
        
        List<String> columns = new ArrayList<>();
        schema.get("columns").forEach(col -> columns.add(col.asText()));
        return columns;
    }
    
    /**
     * Build CREATE TABLE SQL with proper columns (all as STRING for simplicity)
     * Includes _produced_ts for latency measurement
     */
    private static String buildCreateTableSQL(String topic, List<String> columns) {
        StringBuilder sql = new StringBuilder();
        sql.append(String.format("CREATE TABLE IF NOT EXISTS `%s` (", topic));
        
        // Add all columns as STRING type
        for (int i = 0; i < columns.size(); i++) {
            sql.append(String.format("`%s` STRING", columns.get(i)));
            sql.append(", ");
        }
        
        // Add _produced_ts for latency measurement
        sql.append("`_produced_ts` BIGINT");
        
        sql.append(") WITH (");
        sql.append("  'format-version' = '2',");
        sql.append("  'write.format.default' = 'parquet',");
        sql.append("  'write.target-file-size-bytes' = '134217728',");
        sql.append("  'write.parquet.compression-codec' = 'snappy'");
        sql.append(")");
        
        return sql.toString();
    }
    
    /**
     * Build INSERT SQL that extracts JSON fields
     * Includes _produced_ts extraction
     */
    private static String buildInsertSQL(String topic, List<String> columns) {
        StringBuilder sql = new StringBuilder();
        sql.append(String.format("INSERT INTO `%s` SELECT ", topic));
        
        // Extract each field from JSON
        for (int i = 0; i < columns.size(); i++) {
            sql.append(String.format("JSON_VALUE(json_data, '$.%s')", columns.get(i)));
            sql.append(", ");
        }
        
        // Extract _produced_ts as BIGINT
        sql.append("CAST(JSON_VALUE(json_data, '$._produced_ts') AS BIGINT)");
        
        sql.append(" FROM kafka_raw_stream");
        return sql.toString();
    }
    
    public static void main(String[] args) throws Exception {
    
        // Disable Iceberg metrics to avoid conflict
        System.setProperty("iceberg.flink.metrics.enabled", "false");    
        
        // Get topic from command line argument (if provided)
        String topic;
        if (args.length > 0) {
            topic = args[0];
            System.out.println("Processing topic from argument: " + topic);
        } else {
            // Fallback: auto-discover and use first topic
            List<String> topics = discoverTopics();
            if (topics.isEmpty()) {
                System.err.println("ERROR: No schema files found in " + SCHEMA_PATH);
                System.exit(1);
            }
            topic = topics.get(0);
            System.out.println("No argument provided, using first discovered topic: " + topic);
        }
        
        // Load schema for this topic
        List<String> columns = loadSchema(topic);
        System.out.println("Loaded schema with " + columns.size() + " columns");
        
        // Create execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // env.setParallelism(1); // comment or it will override flink-conf.yaml
        
        // Enable checkpointing (important for Iceberg exactly-once semantics)
        // Checkpoint interval is configured via flink-conf.yaml: execution.checkpointing.interval
        env.enableCheckpointing(5000); // Default 5 seconds, comment or it will override flink-conf.yaml
        env.getCheckpointConfig().setCheckpointStorage("file://" + CHECKPOINT_PATH + topic);
        
        // Create Table environment for Iceberg (Flink 1.18 style)
        EnvironmentSettings settings = EnvironmentSettings
            .newInstance()
            .inStreamingMode()
            .build();
        
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);
        
        // Configure Iceberg catalog
        tableEnv.executeSql(
            "CREATE CATALOG iceberg_catalog WITH (" +
            "  'type' = 'iceberg'," +
            "  'catalog-type' = 'hadoop'," +
            "  'warehouse' = '" + WAREHOUSE_PATH + "'" +
            ")"
        );
        
        // Use the Iceberg catalog
        tableEnv.executeSql("USE CATALOG iceberg_catalog");
        
        // Create database if not exists
        tableEnv.executeSql("CREATE DATABASE IF NOT EXISTS " + ICEBERG_NAMESPACE);
        tableEnv.executeSql("USE " + ICEBERG_NAMESPACE);
        
        System.out.println("Iceberg catalog configured:");
        System.out.println("  Warehouse: " + WAREHOUSE_PATH);
        System.out.println("  Database: " + ICEBERG_NAMESPACE);
        System.out.println("  Table: " + topic);
        
        // Kafka consumer properties
        Properties properties = new Properties();
        String bootstrapServers = "localhost:9092";

        properties.setProperty("bootstrap.servers", bootstrapServers);
        System.out.println("Using Kafka bootstrap servers: " + bootstrapServers);
        properties.setProperty("group.id", "flink-iceberg-consumer-" + topic);
        properties.setProperty("auto.offset.reset", "earliest");
        // Create Kafka consumer
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(
            topic,
            new SimpleStringSchema(),
            properties
        );
        
        consumer.setStartFromEarliest();
        
        // Create data stream from Kafka
        DataStream<String> stream = env.addSource(consumer)
            .name("Kafka-Source-" + topic)
            .uid("kafka-source-" + topic);
        
        // Register the stream as a temporary view for SQL operations
        tableEnv.createTemporaryView("kafka_raw_stream", stream, $("json_data"));
        
        // Create Iceberg table with proper columns
        String createTableSQL = buildCreateTableSQL(topic, columns);
        tableEnv.executeSql(createTableSQL);
        System.out.println("Iceberg table created/verified: " + topic);
        
        // Insert stream data into Iceberg table with JSON parsing
        String insertSQL = buildInsertSQL(topic, columns);
        TableResult result = tableEnv.executeSql(insertSQL);
        
        System.out.println("Streaming query started. Writing to Iceberg table: " + ICEBERG_NAMESPACE + "." + topic);
//        System.out.println("Press Ctrl+C to stop...");
        
        // CRITICAL: Wait for the streaming job (blocks until cancelled/failed)
//        result.await();
    }
}
