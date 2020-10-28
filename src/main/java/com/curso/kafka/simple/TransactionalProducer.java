package com.curso.kafka.simple;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.ProducerFencedException;

import com.curso.kafka.util.Constants;
import com.curso.kafka.util.TopicCreator;

public class TransactionalProducer {

	public static final String TOPIC = "topic-transactions";
	public static List<String> CITIES = Arrays.asList("madrid", "barcelona", "burgos");

	public static void main(String[] args) throws InterruptedException, ExecutionException {

		TopicCreator.createTopics(Constants.BROKER_LIST, TOPIC);

		final Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Constants.BROKER_LIST);
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, Constants.DEFAULT_SERIALIZER.getName());
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, Constants.DEFAULT_SERIALIZER.getName());
		// Trasaction configuration
		props.put(ProducerConfig.ACKS_CONFIG, "all");
		props.put(ProducerConfig.CLIENT_ID_CONFIG, TransactionalProducer.class.getName() + TOPIC);
		props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
		props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, TransactionalProducer.class.getName() + TOPIC);
		props.put(ProducerConfig.RETRIES_CONFIG, 3);
		// Permite garantizar el orden
		props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1);

		final Producer<String, String> producer = new KafkaProducer<>(props);
		producer.initTransactions();

		Thread thread = new Thread(producer::close);
		Runtime.getRuntime().addShutdownHook(thread);
		
		int i = 1;
		Random random = new Random();
		
		while (true) {
			try {
				producer.beginTransaction();
				System.out.println("Inicio de transacción ...");
				for (int j = 0; j < 5; j++) {
					ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC,
							CITIES.get(random.nextInt(CITIES.size())), "String " + i++);
					// Garantía síncrona.
					producer.send(record).get();
					System.out.println("Record : " + record.toString());
					Thread.sleep(1000);
				}
				producer.commitTransaction();
				System.out.println("Transacción confirmada!");
			} catch (ProducerFencedException e) {
				producer.abortTransaction();
			}
		}
	}
}
