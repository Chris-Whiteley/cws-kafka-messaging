package com.cwsoft.messaging.kafka;

import com.cwsoft.messaging.ClosableAbstractChunkingProducer;
import com.cwsoft.messaging.chunk.Chunk;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

@Slf4j
public abstract class AbstractKafkaChunkingProducer<T> extends ClosableAbstractChunkingProducer<T> {

    private final KafkaProducer<String, String> kafkaProducer;

    // Constructor initializes Kafka producer with provided properties
    protected AbstractKafkaChunkingProducer(Properties kafkaProperties, int maxMessageSize) {
        super(maxMessageSize);
        this.kafkaProducer = new KafkaProducer<>(applyDefaultProperties(kafkaProperties));
    }

    /**
     * Applies default Kafka properties if not already set by the user.
     *
     * @param config the user-provided configuration
     * @return a configuration object with defaults applied
     */
    private Properties applyDefaultProperties(Properties config) {
        Properties defaultProps = new Properties();
        defaultProps.put("bootstrap.servers", "localhost:9092");
        defaultProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        defaultProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        // Allow user-provided properties to override defaults
        defaultProps.putAll(config);
        return defaultProps;
    }

    protected final void sendChunk(Chunk chunk) {
        try {
            String encodedChunk = chunk.encodeToJson();

            // Create a Kafka producer record
            ProducerRecord<String, String> record = new ProducerRecord<>(chunk.getDestination(), chunk.getName(), encodedChunk);

            // Send message asynchronously
            kafkaProducer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    log.error("Failed to send chunk for message [{}] with index [{}] to Kafka topic [{}]", chunk.getName(), chunk.getIndex(), chunk.getDestination(), exception);
                    throw new RuntimeException("Failed to send chunk to Kafka", exception);
                }

                log.debug("Chunk for message [{}] with index [{}] successfully sent to Kafka topic [{}] - Partition [{}], Offset [{}]",
                        chunk.getName(), chunk.getIndex(), chunk.getDestination(), metadata.partition(), metadata.offset());
            });
        } catch (Exception e) {
            log.error("Error while producing message chunk for message [{}] with index [{}] to Kafka topic [{}]", chunk.getName(), chunk.getIndex(), chunk.getDestination(), e);
            throw new RuntimeException("Kafka message production failed for message: " + chunk.getName(), e);
        }
    }

    /**
     * Shuts down the Kafka producer when no longer needed.
     */
    @Override
    public void close() {
        try {
            kafkaProducer.close();
            log.debug("Kafka producer successfully closed");
        } catch (Exception e) {
            log.error("Error while closing Kafka producer", e);
        }
    }
}
