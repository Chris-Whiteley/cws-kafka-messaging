package com.cwsoft.messaging.kafka;

import lombok.Builder;
import lombok.NonNull;

import java.util.Properties;

@Builder
public class ProducerConfig {
    @NonNull
    private String bootstrapServers;

    @Builder.Default
    private String keySerializer = "org.apache.kafka.common.serialization.StringSerializer";
    @Builder.Default
    private String valueSerializer = "org.apache.kafka.common.serialization.StringSerializer";

    @NonNull
    private String securityProtocol;

    private String truststorePath;
    private String truststorePassword;

    public Properties getProperties() {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", bootstrapServers);
        properties.put("key.serializer", keySerializer);
        properties.put("value.serializer", valueSerializer);
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
