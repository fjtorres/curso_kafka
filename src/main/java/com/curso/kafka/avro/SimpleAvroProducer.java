package com.curso.kafka.avro;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import com.curso.kafka.util.Constants;
import com.curso.kafka.util.OpenWeatherMap;
import com.curso.kafka.util.TopicCreator;
import com.curso.kafka.avro.model.Clima;

public class SimpleAvroProducer {

	public static final String TOPIC = "topic-avro";
	private static final String CITY = "madrid";

	public static void main(String[] args) throws InterruptedException, ExecutionException, IOException {

		TopicCreator.createTopics(Constants.BROKER_LIST, TOPIC);

		final Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Constants.BROKER_LIST);
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, Constants.DEFAULT_SERIALIZER.getName());
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());

		final Producer<String, byte[]> producer = new KafkaProducer<>(props);

		Runtime.getRuntime().addShutdownHook(new Thread(producer::close));

		while (true) {
			final Clima weather = OpenWeatherMap.getWeatherFromOpenWeatherMap(CITY);
			producer.send(new ProducerRecord<>(TOPIC, CITY, weather.toByteBuffer().array()));
			Thread.sleep(1500);
		}
	}
}
