package com.curso.kafka.avro;

import java.io.IOException;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.curso.kafka.avro.model.Clima;
import com.curso.kafka.util.Constants;
import com.curso.kafka.util.OpenWeatherMap;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;

public class SchemaRegistryProducer {

	public static final String CITY = "madrid";
	public static final String TOPIC = "avro-clima-schema-registry";

	public static void main(String[] args) throws InterruptedException, IOException {

		final Properties properties = new Properties();
		properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Constants.BROKER_LIST);
		properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, Constants.DEFAULT_SERIALIZER.getName());
		properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
		properties.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");

		final KafkaProducer<String, Clima> producer = new KafkaProducer<>(properties);

		Runtime.getRuntime().addShutdownHook(new Thread(producer::close));

		while (true) {
			final Clima clima = OpenWeatherMap.getWeatherFromOpenWeatherMap(CITY);
			final ProducerRecord<String, Clima> record = new ProducerRecord<>(TOPIC, CITY, clima);
			producer.send(record);
			Thread.sleep(1500);
		}
	}
}
