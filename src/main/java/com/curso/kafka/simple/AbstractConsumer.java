package com.curso.kafka.simple;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public abstract class AbstractConsumer {
	protected static void logRecord(ConsumerRecord<String, String> record) {
		System.out.printf("partition = %2d offset = %5d key = %7s timestamp = %8s value = %12s\n", record.partition(),
				record.offset(), record.key(), String.valueOf(record.timestamp()), record.value());
	}
}
