package com.curso.kafka.simple;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import com.curso.kafka.util.Constants;

/**
 * Con este consumer con autocommit manual se da el caso de que si paramos la
 * ejecución mientras esta esperando no se lleguen a confirmar los ultimos
 * mensajes leidos y se repitan en la siguiente ejecución.
 * 
 * @author fjtorres
 *
 */
public class ManualConsumer extends AbstractConsumer {

	public static void main(String[] args) throws InterruptedException {

		final Properties props = new Properties();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Constants.KAFKA_HOST);
		props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
		props.put(ConsumerConfig.GROUP_ID_CONFIG, "manual-consumer");
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, Constants.DEFAULT_DESERIALIZER.getName());
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, Constants.DEFAULT_DESERIALIZER.getName());

		final KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

		Runtime.getRuntime().addShutdownHook(new Thread(consumer::close));

		consumer.subscribe(Collections.singletonList(SimpleProducer.TOPIC));

		final Duration interval = Duration.ofMillis(100);

		while (true) {
			consumer.poll(interval).forEach(ManualConsumer::logRecord);
			Thread.sleep(5000);
			consumer.commitSync();
		}
	}
}
