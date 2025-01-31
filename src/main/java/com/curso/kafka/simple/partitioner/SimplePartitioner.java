package com.curso.kafka.simple.partitioner;

import java.util.Map;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

public class SimplePartitioner implements Partitioner {
	
	@Override
	public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
		return Math.abs(key.hashCode() % cluster.partitionCountForTopic(topic));
	}

	@Override
	public void close() {
	}

	@Override
	public void configure(Map<String, ?> conf) {
	}

}
