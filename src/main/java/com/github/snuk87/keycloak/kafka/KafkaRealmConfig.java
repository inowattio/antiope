package com.github.snuk87.keycloak.kafka;

import com.fasterxml.jackson.databind.JsonNode;

public record KafkaRealmConfig(
        String realmName,
        String bootstrapServer,
        String topic,
        String clientId
) {

    public static KafkaRealmConfig from(JsonNode node) {
        String realmName = node.get("realmName").asText();
        String brokerIp = node.get("brokerIp").asText();
        String brokerPort = node.get("brokerPort").asText();
        String topic = node.get("topic").asText();
        String clientId = node.get("clientId").asText();

        return new KafkaRealmConfig(
                realmName,
                brokerIp + ":" + brokerPort,
                topic,
                clientId
        );
    }
}
