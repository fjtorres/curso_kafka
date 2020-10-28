package com.curso.kafka.avro;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import com.curso.kafka.avro.model.Clima;
import com.curso.kafka.simple.AbstractConsumer;
import com.curso.kafka.util.Constants;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;

public class SchemaRegistryConsumer extends AbstractConsumer {

	public static void main(String[] args) {

		final Properties props = new Properties();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Constants.KAFKA_HOST);
		props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
		props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "100");
		props.put(ConsumerConfig.GROUP_ID_CONFIG, "schema-registry-consumer");
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, Constants.DEFAULT_DESERIALIZER.getName());
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
		props.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");

		final KafkaConsumer<String, Clima> consumer = new KafkaConsumer<>(props);

		Runtime.getRuntime().addShutdownHook(new Thread(consumer::close));

		consumer.subscribe(Collections.singletonList(SchemaRegistryProducer.TOPIC));

		final Duration interval = Duration.ofSeconds(1);

		while (true) {
			consumer.poll(interval).forEach(record -> {
				System.out.println(record.value());
			});
		}
	}
}
