package com.cwsoft.messaging.kafka;

import com.cwsoft.messaging.ClosableAbstractChunkingConsumer;
import com.cwsoft.messaging.chunk.Chunk;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Iterator;
import java.util.Properties;

@Slf4j
public abstract class AbstractKafkaChunkingConsumer<T> extends ClosableAbstractChunkingConsumer<T> {

    private final KafkaConsumer<String, String> kafkaConsumer;
    private final String topic;
    private Iterator<ConsumerRecord<String, String>> recordIterator;

    /**
     * Constructor to initialize KafkaConsumer with properties and topic.
     *
     * @param properties Kafka consumer configuration properties.
     * @param topic      The topic to consume messages from.
     */
    protected AbstractKafkaChunkingConsumer(Properties properties, String topic) {
        this.kafkaConsumer = new KafkaConsumer<>(applyDefaultProperties(properties));
        this.topic = topic;
        kafkaConsumer.subscribe(Collections.singletonList(topic));
        log.debug("Subscribed to topic [{}]", topic);
    }

    /**
     * Applies default Kafka properties if not already set by the user.
     *
     * @param config The user-provided configuration properties.
     * @return A configuration object with defaults applied.
     */
    private Properties applyDefaultProperties(Properties config) {
        Properties defaultProps = new Properties();
        defaultProps.put("bootstrap.servers", "localhost:9092");
        defaultProps.put("group.id", "default-group-id");
        defaultProps.put("enable.auto.commit", "true");
        defaultProps.put("auto.commit.interval.ms", "1000");
        defaultProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        defaultProps.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        // Allow user-provided properties to override defaults
        defaultProps.putAll(config);
        return defaultProps;
    }

    /**
     * Provides the source of the messages (in this case, the Kafka topic name).
     */
    @Override
    protected final String getSource() {
        return topic;
    }

    @Override
    protected final Chunk retrieveChunk(Duration timeout) {
        if (recordIterator == null || !recordIterator.hasNext()) {
            log.debug("Polling Kafka topic [{}] with timeout [{}]", topic, timeout);
            ConsumerRecords<String, String> records = kafkaConsumer.poll(timeout);

            if (records.isEmpty()) {
                log.warn("No messages received from topic [{}] within the timeout period", topic);
                return null;
            }
            recordIterator = records.iterator();
        }

        if (recordIterator.hasNext()) {
            ConsumerRecord<String, String> record = recordIterator.next();
            log.debug("Received message from topic [{}]: {}", topic, record.value());
            return Chunk.decodeFromJson(record.value());
        }

        return null;
    }

    /**
     * Decodes the message into the desired type `T`. Must be implemented by subclasses.
     *
     * @param encodedMessage The raw message received from Kafka.
     * @return Decoded message of type `T`.
     */
    @Override
    protected abstract T decode(String encodedMessage);

    /**
     * Closes the Kafka consumer gracefully.
     */
    @Override
    public void close() {
        log.debug("Closing Kafka consumer for topic [{}]", topic);
        kafkaConsumer.close();
    }
}
