package com.curso.kafka.util;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

public final class Constants {

	public static final String KAFKA_HOST = "localhost:9092";
	public static final String BROKER_LIST = "localhost:9091,localhost:9092,localhost:9093";
	public static final Class<? extends Serializer<?>> DEFAULT_SERIALIZER = StringSerializer.class;
	public static final Class<? extends Deserializer<?>> DEFAULT_DESERIALIZER = StringDeserializer.class;
	
	private Constants() {}
}
