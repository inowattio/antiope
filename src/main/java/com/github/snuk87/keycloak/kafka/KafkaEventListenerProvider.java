package com.github.snuk87.keycloak.kafka;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.jboss.logging.Logger;
import org.keycloak.events.Event;
import org.keycloak.events.EventListenerProvider;
import org.keycloak.events.EventType;
import org.keycloak.events.admin.AdminEvent;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class KafkaEventListenerProvider implements EventListenerProvider {

	private static final Logger LOG = Logger.getLogger(KafkaEventListenerProvider.class);
	private final List<EventType> events;
	private final ObjectMapper mapper;
	private final KafkaConfigService kafkaConfigService;
	private final KeycloakSessionHelper keycloakSessionHelper;
	private final KafkaProducerManager kafkaProducerManager;

	public KafkaEventListenerProvider(KeycloakSessionHelper keycloakSessionHelper, KafkaConfigService kafkaConfigService, KafkaProducerManager kafkaProducerManager) {

		this.events = new ArrayList<>();
        this.keycloakSessionHelper =  keycloakSessionHelper;
		this.kafkaProducerManager = kafkaProducerManager;
		this.kafkaConfigService = kafkaConfigService;
		mapper = new ObjectMapper();

		for (String event : kafkaConfigService.getEvents()) {
			try {
				EventType eventType = EventType.valueOf(event.toUpperCase());
				this.events.add(eventType);
			} catch (IllegalArgumentException e) {
				LOG.debug("Ignoring event >" + event + "<. Event does not exist.");
			}
		}

	}

	private void produceEvent(String eventAsString, String realmName)
			throws InterruptedException, ExecutionException, TimeoutException {

		String topic = this.kafkaProducerManager.getTopic(realmName);
		
		if (topic == null) {
			LOG.warn("No Kafka topic configured for realm: " + realmName);
			return;
		}

		Producer<String, String> producer = this.kafkaProducerManager.getProducer(realmName);
		if (producer == null) {
			LOG.warn("Kafka producer not ready for realm: " + realmName);
			return;
		}

		ProducerRecord<String, String> record = new ProducerRecord<>(topic, eventAsString);
		Future<RecordMetadata> metaData = producer.send(record);

		metaData.get(30, TimeUnit.SECONDS);
		LOG.info("TOPIC: " + topic);
		LOG.info("PRODUCER: " + producer);
		LOG.info("REALM_NAME: " + realmName);
	}

	@Override
	public void onEvent(Event event) {
		if (events.contains(event.getType())) {
			try {
				String eventAsString = mapper.writeValueAsString(event);
				String realmName = keycloakSessionHelper.getRealmName(eventAsString);
				produceEvent(eventAsString, realmName);

			} catch (JsonProcessingException | ExecutionException | TimeoutException e) {
				LOG.error(e.getMessage(), e);
			} catch (InterruptedException e) {
				LOG.error(e.getMessage(), e);
				Thread.currentThread().interrupt();
			}
		}
	}

	@Override
	public void onEvent(AdminEvent event, boolean includeRepresentation) {
		if (kafkaConfigService.getTopicAdminEvents() != null) {
			try {
				String eventAsString = mapper.writeValueAsString(event);
				String realmName = keycloakSessionHelper.getRealmName(eventAsString);
				produceEvent(eventAsString, realmName);
			} catch (JsonProcessingException | ExecutionException | TimeoutException e) {
				LOG.error(e.getMessage(), e);
			} catch (InterruptedException e) {
				LOG.error(e.getMessage(), e);
				Thread.currentThread().interrupt();
			}
		}
	}

	@Override
	public void close() {
		// ignore
	}

}
