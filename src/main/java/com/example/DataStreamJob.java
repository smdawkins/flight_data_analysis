package com.example;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import com.example.FlightSegment;
import com.example.FlightOfferRecord;
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

import org.apache.flink.api.common.restartstrategy.RestartStrategies;






import java.util.Properties;

public class DataStreamJob {
    public static void main(String[] args) throws Exception {
        // Set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);

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
		// Set restart strategy (3 retries with 10-second delay)
		env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 10000));
		// Enable exactly-once processing
		kafkaConsumer.setCommitOffsetsOnCheckpoints(true);

        // Add the Kafka consumer as a source
        DataStream<String> stream = env.addSource(kafkaConsumer);

        // Create an ObjectMapper for JSON parsing
        ObjectMapper objectMapper = new ObjectMapper();

        // Parse and flatten the flight data
        // Parse and flatten the flight data
		DataStream<FlightOfferRecord> flattenedStream = stream
			.flatMap((String json, Collector<FlightOfferRecord> out) -> {
				try {
					System.out.println("Processing message: " + json);
					if (json == null || json.trim().isEmpty()) {
						return;
					}

					// Parse the JSON into a JsonNode
					JsonNode offer = objectMapper.readTree(json);

					// Extract required fields from the offer
					String id = offer.get("id").asText();
					String lastTicketingDate = offer.get("lastTicketingDate").asText();
					int numberOfBookableSeats = offer.get("numberOfBookableSeats").asInt();
					String totalPrice = offer.get("price").get("total").asText();
					String currency = offer.get("price").get("currency").asText();
					String validatingAirlineCodes = offer.get("validatingAirlineCodes").toString();

					// Extract segments from itineraries
					JsonNode itineraries = offer.get("itineraries");
					if (itineraries == null || !itineraries.isArray()) {
						return;
					}

					for (JsonNode itinerary : itineraries) {
						JsonNode segments = itinerary.get("segments");

						if (segments == null || !segments.isArray()) {
							continue;
						}

						int flightSegments = segments.size();
						String departureTime = segments.get(0).get("departure").get("at").asText();
						String arrivalTime = segments.get(segments.size() - 1).get("arrival").get("at").asText();
						String duration = itinerary.get("duration").asText();

						// Extract IATA codes
						String departureIataCode = segments.get(0).get("departure").get("iataCode").asText();
						String arrivalIataCode = segments.get(segments.size() - 1).get("arrival").get("iataCode").asText();

						// Create a record for PostgreSQL
						FlightOfferRecord record = new FlightOfferRecord(
							id, lastTicketingDate, numberOfBookableSeats, duration,
							totalPrice, currency, validatingAirlineCodes, flightSegments,
							departureTime, arrivalTime, departureIataCode, arrivalIataCode
						);

						out.collect(record);
					}
				} catch (Exception e) {
					System.err.println("Failed to process message: " + json + ", error: " + e.getMessage());
				}
			})
			.returns(org.apache.flink.api.common.typeinfo.TypeInformation.of(FlightOfferRecord.class));


        // Print the flattened data to the console
		
        flattenedStream.print();
		flattenedStream.writeAsText("/opt/flink/flight-data-flink/output/processed-flight-data.txt", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
		
		// Sink: Write the flattened data to PostgreSQL
        flattenedStream.addSink(JdbcSink.sink(
			"INSERT INTO flight_offers (id, last_ticketing_date, number_of_bookable_seats, duration, total_price, currency, validating_airline_codes, flight_segments, departure_time, arrival_time, departure_iata_code, arrival_iata_code, insert_time) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
			new JdbcStatementBuilder<FlightOfferRecord>() {
				@Override
				public void accept(PreparedStatement ps, FlightOfferRecord record) throws SQLException {
					ps.setString(1, record.getId());
					ps.setDate(2, java.sql.Date.valueOf(record.getLastTicketingDate()));
					ps.setInt(3, record.getNumberOfBookableSeats());
					ps.setString(4, record.getDuration());
					ps.setBigDecimal(5, new java.math.BigDecimal(record.getTotalPrice()));
					ps.setString(6, record.getCurrency());
					ps.setString(7, record.getValidatingAirlineCodes());
					ps.setInt(8, record.getFlightSegments());
					ps.setTimestamp(9, Timestamp.valueOf(record.getDepartureTime().replace("T", " ")));
					ps.setTimestamp(10, Timestamp.valueOf(record.getArrivalTime().replace("T", " ")));
					ps.setString(11, record.getDepartureIataCode());
					ps.setString(12, record.getArrivalIataCode());
					ps.setTimestamp(13, Timestamp.valueOf(record.getInsertTime()));
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
