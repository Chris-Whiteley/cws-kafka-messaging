package com.cwsoft.messaging.kafka;

import com.cwsoft.messaging.ClosableAbstractConsumer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Iterator;
import java.util.Properties;

@Slf4j
public abstract class AbstractKafkaConsumer<T> extends ClosableAbstractConsumer<T> {

    private final KafkaConsumer<String, String> kafkaConsumer;
    private final String topic;
    private Iterator<org.apache.kafka.clients.consumer.ConsumerRecord<String, String>> recordIterator;

    /**
     * Constructor to initialize KafkaConsumer with properties and topic.
     *
     * @param properties Kafka consumer configuration properties.
     * @param topic      The topic to consume messages from.
     */
    public AbstractKafkaConsumer(Properties properties, String topic) {
        this.kafkaConsumer = new KafkaConsumer<>(properties);
        this.topic = topic;
        kafkaConsumer.subscribe(Collections.singletonList(topic));
        log.debug("Subscribed to topic [{}]", topic);
    }

    /**
     * Provides the source of the messages (in this case, the Kafka topic name).
     */
    @Override
    protected String getSource() {
        return topic;
    }

    /**
     * Retrieves a single encoded message from the Kafka topic, blocking for the specified timeout duration.
     *
     * This implementation ensures that multiple messages retrieved in a single `poll()` are processed
     * one at a time using an iterator.
     *
     * @param timeout the maximum duration to wait for a message.
     * @return the encoded message as a string, or null if no message is available within the timeout.
     */
    @Override
    protected String retrieveMessage(Duration timeout) {
        log.debug("Polling Kafka topic [{}] with timeout [{}]", topic, timeout);

        // Check if there are more records in the iterator from the last poll
        if (recordIterator == null || !recordIterator.hasNext()) {
            ConsumerRecords<String, String> records = kafkaConsumer.poll(timeout);
            if (records.isEmpty()) {
                log.warn("No messages received from topic [{}] within the timeout period", topic);
                return null;
            }
            log.debug("Retrieved [{}] messages from topic [{}]", records.count(), topic);
            recordIterator = records.iterator();
        }

        // Process the next record
        if (recordIterator.hasNext()) {
            var record = recordIterator.next();
            log.debug("Processing message from topic [{}]: {}", topic, record.value());
            return record.value();
        }

        // This point should not be reached, but return null as a fallback
        log.warn("Iterator unexpectedly empty after poll on topic [{}]", topic);
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
