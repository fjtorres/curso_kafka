package com.curso.kafka.simple;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.curso.kafka.util.Constants;
import com.curso.kafka.util.TopicCreator;

public class SimpleProducer {

	public static final String TOPIC = "topicSimple";

	public static void main(String[] args) throws InterruptedException, ExecutionException {

		TopicCreator.createTopics(Constants.BROKER_LIST, TOPIC);

		final Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Constants.BROKER_LIST);
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, Constants.DEFAULT_SERIALIZER.getName());
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, Constants.DEFAULT_SERIALIZER.getName());

		final Producer<String, String> producer = new KafkaProducer<>(props);

		for (int id = 0; id < 5000; id++) {
			String key = String.format("key[%d]", id);
			String message = String.format("message[%d]", id);
			System.out.println("Sending message with: " + key);
			producer.send(new ProducerRecord<>(TOPIC, key, message));
			Thread.sleep(1000);
		}

		producer.flush();
		producer.close();
	}
}
