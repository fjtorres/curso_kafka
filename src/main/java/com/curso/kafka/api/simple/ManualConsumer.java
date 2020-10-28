package com.curso.kafka.api.simple;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import com.curso.kafka.api.Constants;

/**
 * Con este consumer con autocommit manual se da el caso de que si paramos la
 * ejecución mientras esta esperando no se lleguen a confirmar los ultimos
 * mensajes leidos y se repitan en la siguiente ejecución.
 * 
 * @author fjtorres
 *
 */
public class ManualConsumer {

	private static final AtomicBoolean closed = new AtomicBoolean(false);// para cerrar al matar el proceso

	public static void main(String[] args) throws InterruptedException {

		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				System.out.println("Shutting down");
				closed.set(true);
			}
		});

		final Properties props = new Properties();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Constants.KAFKA_HOST);
		props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
		props.put(ConsumerConfig.GROUP_ID_CONFIG, "manual-consumer");
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, Constants.DEFAULT_DESERIALIZER.getName());
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, Constants.DEFAULT_DESERIALIZER.getName());

		final KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
		consumer.subscribe(Collections.singletonList(SimpleProducer.TOPIC));

		final Duration interval = Duration.ofMillis(100);

		while (!closed.get()) {
			consumer.poll(interval).forEach(ManualConsumer::logRecord);
			Thread.sleep(5000);
			consumer.commitSync();
		}

		consumer.close();
	}

	private static void logRecord(ConsumerRecord<String, String> record) {
		System.out.printf("partition = %2d offset = %5d key = %7s timestamp = %8s value = %12s\n", record.partition(),
				record.offset(), record.key(), String.valueOf(record.timestamp()), record.value());
	}
}
