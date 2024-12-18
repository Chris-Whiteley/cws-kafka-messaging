package com.cwsoft.messaging.kafka;

import com.cwsoft.messaging.ClosableAbstractProducer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

@Slf4j
public abstract class AbstractKafkaProducer<T> extends ClosableAbstractProducer<T> {

    private final KafkaProducer<String, String> kafkaProducer;

    // Constructor initializes Kafka producer with provided properties
    public AbstractKafkaProducer(Properties kafkaProperties) {
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

    /**
     * Sends the encoded message to the Kafka topic (destination).
     */
    @Override
    protected void sendToDestination(String messageName, String destination, String encodedMessage) {
        try {
            log.debug("Sending message [{}] to Kafka topic [{}]", messageName, destination);

            ProducerRecord<String, String> record = new ProducerRecord<>(destination, messageName, encodedMessage);

            // Send message asynchronously
            kafkaProducer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    log.error("Failed to send message [{}] to Kafka topic [{}]", messageName, destination, exception);
                    throw new RuntimeException("Failed to send message to Kafka", exception);
                }

                log.debug("Message [{}] successfully sent to Kafka topic [{}] - Partition [{}], Offset [{}]",
                        messageName, destination, metadata.partition(), metadata.offset());
            });
        } catch (Exception e) {
            log.error("Error while producing message [{}] to Kafka topic [{}]", messageName, destination, e);
            throw new RuntimeException("Kafka message production failed for message: " + messageName, e);
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
