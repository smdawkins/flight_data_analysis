package com.example;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import com.example.FlightSegment;
import org.apache.flink.util.Collector;
import org.apache.flink.core.fs.FileSystem;


import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;





import java.util.Properties;

public class DataStreamJob {
    public static void main(String[] args) throws Exception {
        // Set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Kafka consumer properties
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "flight-data-consumer");
		properties.setProperty("auto.offset.reset", "latest"); 
		

        // Create a Kafka consumer
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
            "flight-data",                      // Kafka topic
            new SimpleStringSchema(),           // Deserialization schema
            properties                          // Consumer properties
        );
		
		env.enableCheckpointing(60000); 

        // Add the Kafka consumer as a source
        DataStream<String> stream = env.addSource(kafkaConsumer);

        // Create an ObjectMapper for JSON parsing
        ObjectMapper objectMapper = new ObjectMapper();

        // Parse and flatten the flight data
        DataStream<FlightSegment> flattenedStream = stream
            .flatMap((String json, Collector<FlightSegment> out) -> {
				
				try {
					System.out.println("Processing message: " + json);
					// Skip empty messages
					if (json == null || json.trim().isEmpty()) {
						return;
					}
                // Parse the JSON into a JsonNode
                JsonNode rootNode = objectMapper.readTree(json);
                JsonNode itineraries = rootNode.get("itineraries");
				if (itineraries == null || !itineraries.isArray()) {
					System.out.println("No valid 'itineraries' array found.");
					return;
				}

				// Iterate through itineraries
				for (JsonNode itinerary : itineraries) {
					JsonNode segments = itinerary.get("segments");
					if (segments == null || !segments.isArray()) {
						System.out.println("No valid 'segments' array found.");
						continue;
					}

					// Iterate through segments
					for (JsonNode segment : segments) {
						String origin = segment.get("departure").get("iataCode").asText();
						String destination = segment.get("arrival").get("iataCode").asText();
						String departureDate = segment.get("departure").get("at").asText();
						double price = rootNode.get("price").get("grandTotal").asDouble();

						System.out.println("Collected FlightSegment: " + origin + " -> " + destination + ", Price: " + price);
						out.collect(new FlightSegment(origin, destination, departureDate, price));
					}
                }
			} catch (Exception e) {
				// Log and skip malformed messages
				System.err.println("Failed to process message: " + json + ", error: " + e.getMessage());
			}
            })
            .returns(org.apache.flink.api.common.typeinfo.TypeInformation.of(FlightSegment.class)); // Specify the return type

        // Print the flattened data to the console
		
        flattenedStream.print();
		flattenedStream.writeAsText("/opt/flink/flight-data-flink/output/processed-flight-data.txt", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
		
		// Sink: Write the flattened data to PostgreSQL
        flattenedStream.addSink(JdbcSink.sink(
            "INSERT INTO flight_segments (origin, destination, departure_date, price) VALUES (?, ?, ?, ?)",
            new JdbcStatementBuilder<FlightSegment>() {
                @Override
                public void accept(PreparedStatement ps, FlightSegment segment) throws SQLException {
                    ps.setString(1, segment.getOrigin());
                    ps.setString(2, segment.getDestination());
                    ps.setTimestamp(3, Timestamp.valueOf(segment.getDepartureDate().replace("T", " ")));
                    ps.setDouble(4, segment.getPrice());
                }
            },
            JdbcExecutionOptions.builder()
                .withBatchSize(1000)
                .withBatchIntervalMs(200)
                .build(),
            new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                .withUrl("jdbc:postgresql://172.31.224.1:5432/flight_data")
                .withDriverName("org.postgresql.Driver")
                .withUsername("postgres")
                .withPassword("Sd208112!")
                .build()
        ));


        // Execute the Flink job
        env.execute("Flight Data Streaming Job");
    }
}
