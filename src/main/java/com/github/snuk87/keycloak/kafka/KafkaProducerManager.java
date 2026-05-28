package com.github.snuk87.keycloak.kafka;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.producer.Producer;
import org.jboss.logging.Logger;

public class KafkaProducerManager implements AutoCloseable {

	private static final Logger LOG = Logger.getLogger(KafkaProducerManager.class);
    private static final long DEFAULT_REFRESH_INTERVAL_MS = 5000L;

    private final KafkaProducerFactory factory;
    private final Map<String, Object> kafkaProducerProperties;
    private final Map<String, KafkaRealmConfig> realmConfigs = new ConcurrentHashMap<>();
    private final Map<String, Producer<String, String>> producers = new ConcurrentHashMap<>();
    private final AtomicBoolean started = new AtomicBoolean(false);
    private final long refreshIntervalMs;
    private final ScheduledExecutorService scheduler;

    public KafkaProducerManager(KafkaProducerFactory factory, KafkaConfigService kafkaConfigService,
                                Map<String, Object> kafkaProducerProperties) {
        this.factory = Objects.requireNonNull(factory, "KafkaProducerFactory must not be null");
        Objects.requireNonNull(kafkaConfigService, "KafkaConfigService must not be null");
        this.kafkaProducerProperties = Objects.requireNonNull(kafkaProducerProperties,
                "KafkaProducerProperties must not be null");
        this.refreshIntervalMs = Long.parseLong(
            System.getenv().getOrDefault(
                "ANTIOPE_KAFKA_REFRESH_INTERVAL_MS",
                "300000"
            )
        );
        this.scheduler = Executors.newSingleThreadScheduledExecutor(new KafkaReconnectThreadFactory());

        for (KafkaRealmConfig realmConfig : kafkaConfigService.getKafkaRealmConfig()) {
            realmConfigs.put(realmConfig.realmName(), realmConfig);
        }
    }

    public void start() {
        if (started.compareAndSet(false, true)) {
            scheduler.scheduleWithFixedDelay(this::refreshMissingProducers, 0, refreshIntervalMs, TimeUnit.MILLISECONDS);
        }
    }

    public String getTopic(String realmName) {
        KafkaRealmConfig realmConfig = realmConfigs.get(realmName);
        return realmConfig != null ? realmConfig.topic() : null;
    }

    public Producer<String, String> getProducer(String realmName) {
        return producers.get(realmName);
    }

    @Override
    public void close() {
        scheduler.shutdownNow();
        for (Producer<String, String> producer : producers.values()) {
            producer.close();
        }
        producers.clear();
    }

    private void refreshMissingProducers() {
        for (KafkaRealmConfig realmConfig : realmConfigs.values()) {
            if (producers.containsKey(realmConfig.realmName())) {
                continue;
            }

            try {
                Producer<String, String> producer = factory.createProducer(
                        realmConfig.clientId(),
                        realmConfig.bootstrapServer(),
                        kafkaProducerProperties);

                producer.partitionsFor(realmConfig.topic());
                producers.put(realmConfig.realmName(), producer);

                LOG.info("Kafka producer ready for realm "
                        + realmConfig.realmName()
                        + " and topic "
                        + realmConfig.topic());
            
            } catch (Exception e) {
                LOG.warn("Kafka not ready for realm "
                        + realmConfig.realmName(), e);
            }
        }
    }

    private static final class KafkaReconnectThreadFactory implements ThreadFactory {
        @Override
        public Thread newThread(Runnable runnable) {
            Thread thread = new Thread(runnable, "antiope-kafka-reconnect");
            thread.setDaemon(true);
            return thread;
        }
    }
}
