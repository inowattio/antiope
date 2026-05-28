package com.github.snuk87.keycloak.kafka;

import java.util.Map;

import org.jboss.logging.Logger;
import org.keycloak.Config.Scope;
import org.keycloak.events.EventListenerProvider;
import org.keycloak.events.EventListenerProviderFactory;
import org.keycloak.models.KeycloakSession;
import org.keycloak.models.KeycloakSessionFactory;

public class KafkaEventListenerProviderFactory implements EventListenerProviderFactory {

    private static final Logger LOG = Logger.getLogger(KafkaEventListenerProviderFactory.class);
    private static final String ID = "kafka";

    private KafkaEventListenerProvider instance;

    private Map<String, Object> kafkaProducerProperties;
    private KafkaConfigService kafkaConfigService;
    private KafkaProducerManager kafkaProducerManager;
    private KafkaProducerFactory kafkaProducerFactory;

    @Override
    public EventListenerProvider create(KeycloakSession session) {
        if(instance == null) {
            instance = new KafkaEventListenerProvider(new KeycloakSessionHelper(session), 
            kafkaConfigService, kafkaProducerManager);
        }
        return instance;
    }

    @Override
    public String getId() {
        return ID;
    }

    @Override
    public void init(Scope config) {
        LOG.info("Init kafka module ...");
        kafkaConfigService = new KafkaConfigService(config);
        kafkaProducerProperties = KafkaProducerConfig.init(config);
        kafkaProducerFactory = new KafkaStandardProducerFactory();
    }

    @Override
    public void postInit(KeycloakSessionFactory factory) {
        kafkaProducerManager = new KafkaProducerManager(
                kafkaProducerFactory,
                kafkaConfigService,
                kafkaProducerProperties);
        kafkaProducerManager.start();
    }

    @Override
    public void close() {
        if (kafkaProducerManager != null) {
            kafkaProducerManager.close();
        }
    }
}
