package com.cwsoft.messaging.kafka;

import lombok.Builder;
import lombok.NonNull;

import java.util.Properties;

@Builder
public class ConsumerConfig {
    @NonNull
    private String bootstrapServers;

    @NonNull
    private String groupId;

    @Builder.Default
    private String keyDeserializer = "org.apache.kafka.common.serialization.StringDeserializer";
    @Builder.Default
    private String valueDeserializer = "org.apache.kafka.common.serialization.StringDeserializer";

    @NonNull
    private String securityProtocol;

    private String truststorePath;
    private String truststorePassword;

    public Properties getProperties() {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", bootstrapServers);
        properties.put("group.id", groupId);
        properties.put("key.deserializer", keyDeserializer);
        properties.put("value.deserializer", valueDeserializer);
        properties.put("security.protocol", securityProtocol);

        // If SSL is enabled, validate truststore properties
        if ("SSL".equals(securityProtocol)) {
            if (truststorePath == null || truststorePassword == null) {
                throw new IllegalArgumentException("SSL is enabled but truststore properties are missing. " +
                        "Please provide ssl.truststore.location and ssl.truststore.password.");
            }
            properties.put("ssl.truststore.location", truststorePath);
            properties.put("ssl.truststore.password", truststorePassword);
        }

        return properties;
    }
}
