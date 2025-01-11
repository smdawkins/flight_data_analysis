import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

public class StreamingJob {
    public static void main(String[] args) throws Exception {
        // Set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Kafka consumer properties
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "flight-data-consumer");

        // Create a Kafka consumer
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
            "flight-data",                      // Kafka topic
            new SimpleStringSchema(),           // Deserialization schema
            properties                          // Consumer properties
        );

        // Add the Kafka consumer as a source
        DataStream<String> stream = env.addSource(kafkaConsumer);

        // Print the consumed data to the console
        stream.print();

        // Execute the Flink job
        env.execute("Flight Data Streaming Job");
    }
}
