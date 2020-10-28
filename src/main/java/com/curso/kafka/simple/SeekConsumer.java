package com.curso.kafka.simple;

import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import com.curso.kafka.util.Constants;

public class SeekConsumer extends AbstractConsumer {

	public static void main(String[] args) {

		final Properties props = new Properties();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Constants.KAFKA_HOST);
		props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
		props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "100");
		props.put(ConsumerConfig.GROUP_ID_CONFIG, "partition-consumer");
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, Constants.DEFAULT_DESERIALIZER.getName());
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, Constants.DEFAULT_DESERIALIZER.getName());

		final Set<TopicPartition> partitions = new HashSet<>();
		partitions.add(new TopicPartition(SimpleProducer.TOPIC, 0));
		partitions.add(new TopicPartition(SimpleProducer.TOPIC, 1));
		partitions.add(new TopicPartition(SimpleProducer.TOPIC, 2));

		final KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

		Runtime.getRuntime().addShutdownHook(new Thread(consumer::close));

		consumer.assign(partitions); // Instead of subscribe

		// We can define what is done for each partition
		consumer.seekToBeginning(Collections.singleton(new TopicPartition(SimpleProducer.TOPIC, 0)));
		consumer.seekToEnd(Collections.singleton(new TopicPartition(SimpleProducer.TOPIC, 1)));
		consumer.seek(new TopicPartition(SimpleProducer.TOPIC, 2), 20); // Start to read in specific offset

		final Duration interval = Duration.ofSeconds(1);

		while (true) {
			consumer.poll(interval).forEach(SeekConsumer::logRecord);
		}
	}
}
