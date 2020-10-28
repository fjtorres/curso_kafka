package com.curso.kafka.avro;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

import com.curso.kafka.util.Constants;
import com.curso.kafka.avro.model.Clima;
import com.curso.kafka.simple.AbstractConsumer;

public class SimpleAvroConsumer extends AbstractConsumer {

	public static void main(String[] args) {

		final Properties props = new Properties();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Constants.KAFKA_HOST);
		props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
		props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "100");
		props.put(ConsumerConfig.GROUP_ID_CONFIG, "avro-consumer");
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, Constants.DEFAULT_DESERIALIZER.getName());
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());

		final KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(props);

		Runtime.getRuntime().addShutdownHook(new Thread(consumer::close));

		consumer.subscribe(Collections.singletonList(SimpleAvroProducer.TOPIC));

		final Duration interval = Duration.ofSeconds(1);

		while (true) {
			consumer.poll(interval).forEach(record -> {

				try {
					Clima weather = Clima.fromByteBuffer(ByteBuffer.wrap(record.value()));
					System.out.println(weather);
				} catch (IOException e) {
					throw new RuntimeException(e);
				}

			});
		}
	}
}
