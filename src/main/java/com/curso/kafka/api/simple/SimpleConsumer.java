package com.curso.kafka.api.simple;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import com.curso.kafka.api.Constants;

public class SimpleConsumer {

	private static final AtomicBoolean closed = new AtomicBoolean(false);// para cerrar al matar el proceso

	public static void main(String[] args) {

		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				System.out.println("Shutting down");
				closed.set(true);
			}
		});

		final Properties props = new Properties();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Constants.KAFKA_HOST);
		props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
		props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "100");
		props.put(ConsumerConfig.GROUP_ID_CONFIG, "simple-consumer");
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, Constants.DEFAULT_DESERIALIZER.getName());
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, Constants.DEFAULT_DESERIALIZER.getName());

		final KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
		consumer.subscribe(Collections.singletonList(SimpleProducer.TOPIC));

		final Duration interval = Duration.ofSeconds(1);

		while (!closed.get()) {
			consumer.poll(interval).forEach(SimpleConsumer::logRecord);
		}

		consumer.close();
	}

	private static void logRecord(ConsumerRecord<String, String> record) {
		System.out.printf("partition = %2d offset = %5d key = %7s timestamp = %8s value = %12s\n",
				record.partition(),
				record.offset(),
				record.key(),
				String.valueOf(record.timestamp()),
				record.value()
		);
	}
}
