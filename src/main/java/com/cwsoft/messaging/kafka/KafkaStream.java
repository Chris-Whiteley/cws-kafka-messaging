package com.cwsoft.messaging.kafka;

import com.cwsoft.messaging.Stream;
import lombok.Builder;
import lombok.NonNull;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;
import java.util.function.BiConsumer;
import java.util.function.BiPredicate;

/**
 * A wrapper around Kafka Streams providing a flexible interface for message processing.
 *
 * @param <K> the type of the key
 * @param <V> the type of the value
 */
public class KafkaStream<K, V> implements Stream<K, V> {

    private final KStream<K, V> kStream;
    private final KafkaStreams kafkaStreams;

    /**
     * Builder for creating an instance of KafkaStream.
     *
     * @param topicName  the Kafka topic to consume from
     * @param config     the configuration properties for Kafka Streams
     * @param keyClass   the class type of the key
     * @param valueClass the class type of the value
     * @param keySerde   optional custom Serde for the key; defaults to Serdes.serdeFrom(keyClass)
     * @param valueSerde optional custom Serde for the value; defaults to Serdes.serdeFrom(valueClass)
     * @throws IllegalArgumentException if topicName or config is null
     */
    @Builder
    public KafkaStream(
            @NonNull String topicName,
            @NonNull Properties config,
            @NonNull Class<K> keyClass,
            @NonNull Class<V> valueClass,
            Serde<K> keySerde,
            Serde<V> valueSerde
    ) {
        validate(topicName, config);

        StreamsBuilder builder = new StreamsBuilder();

        // Use custom Serde if provided, otherwise fallback to Serdes.serdeFrom
        Serde<K> finalKeySerde = keySerde != null ? keySerde : Serdes.serdeFrom(keyClass);
        Serde<V> finalValueSerde = valueSerde != null ? valueSerde : Serdes.serdeFrom(valueClass);

        this.kStream = builder.stream(
                topicName,
                org.apache.kafka.streams.kstream.Consumed.with(finalKeySerde, finalValueSerde)
        );

        this.kafkaStreams = new KafkaStreams(builder.build(), applyDefaultProperties(config));
    }

    /**
     * Applies default Kafka properties if not already set by the user.
     *
     * @param config the user-provided configuration
     * @return a configuration object with defaults applied
     */
    private Properties applyDefaultProperties(Properties config) {
        Properties defaultProps = new Properties();
        defaultProps.put("application.id", "default-app-id");
        defaultProps.put("bootstrap.servers", "localhost:9092");
        defaultProps.put("default.key.serde", Serdes.String().getClass().getName());
        defaultProps.put("default.value.serde", Serdes.String().getClass().getName());

        // Allow user-provided properties to override defaults
        defaultProps.putAll(config);
        return defaultProps;
    }

    /**
     * Validates required fields.
     *
     * @param topicName the topic name
     * @param config    the Kafka configuration
     */
    private void validate(String topicName, Properties config) {
        if (topicName == null || topicName.isBlank()) {
            throw new IllegalArgumentException("Topic name must not be null or blank");
        }
        if (config == null) {
            throw new IllegalArgumentException("Config properties must not be null");
        }
    }

    @Override
    public Stream<K, V> filter(BiPredicate<K, V> predicate) {
        kStream.filter(predicate::test);
        return this;
    }

    @Override
    public void forEach(BiConsumer<K, V> action) {
        kStream.foreach(action::accept);
    }

    @Override
    public void start() {
        kafkaStreams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));
    }

    @Override
    public void stop() {
        // Kafka Streams does not have a dedicated stop mechanism, so we invoke close.
        kafkaStreams.close();
    }

    @Override
    public void close() {
        kafkaStreams.close();
    }
}
