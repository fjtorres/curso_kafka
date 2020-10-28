package com.curso.kafka.simple;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import com.curso.kafka.util.Constants;

public class TransactionalConsumer extends AbstractConsumer {

	public static void main(String[] args) {

		final Properties props = new Properties();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Constants.KAFKA_HOST);
		props.put(ConsumerConfig.GROUP_ID_CONFIG, "transaction-consumer");
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, Constants.DEFAULT_DESERIALIZER.getName());
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, Constants.DEFAULT_DESERIALIZER.getName());
		// Transactional configuration
		props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");// default "read_uncommitted"
		props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

		final KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
		
		Runtime.getRuntime().addShutdownHook(new Thread(consumer::close));
		
		consumer.subscribe(Collections.singletonList(TransactionalProducer.TOPIC));

		final Duration interval = Duration.ofSeconds(1);

		while (true) {
			ConsumerRecords<String, String> records = consumer.poll(interval);
			records.forEach(TransactionalConsumer::logRecord);

			// Move offset to right position in each partition when commit
			HashMap<TopicPartition, OffsetAndMetadata> partitionsWithOffset = new HashMap<>();
			for (TopicPartition partition : records.partitions()) {
				List<ConsumerRecord<String, String>> list = records.records(partition);
				long offset = list.get(list.size() - 1).offset();
				partitionsWithOffset.put(partition, new OffsetAndMetadata(offset + 1));
			}
			consumer.commitSync(partitionsWithOffset);
		}
	}
}
