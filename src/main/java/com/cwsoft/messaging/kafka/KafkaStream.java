package com.cwsoft.messaging.kafka;

import com.cwsoft.messaging.Stream;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;
import java.util.function.BiConsumer;
import java.util.function.BiPredicate;

public class KafkaStream<K, V> implements Stream<K, V> {

    private final KStream<K, V> kStream;
    private final KafkaStreams kafkaStreams;

    public KafkaStream(String topicName, Properties config, Class<K> keyClass, Class<V> valueClass) {
        StreamsBuilder builder = new StreamsBuilder();
        this.kStream = builder.stream(topicName, org.apache.kafka.streams.kstream.Consumed.with(Serdes.serdeFrom(keyClass), Serdes.serdeFrom(valueClass)));
        this.kafkaStreams = new KafkaStreams(builder.build(), config);
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
    public void close() {
        kafkaStreams.close();
    }
}
