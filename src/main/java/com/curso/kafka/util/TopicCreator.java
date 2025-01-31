package com.curso.kafka.util;

import java.util.Collection;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;

/**
 * Clase de utilidad para crear topics en el cluster de kafka.
 * 
 * Nota: No se crean de forma sincrona por lo que puede darse el caso de que no
 * se hayan creado cuando el producer o consumer se conecte y genere el topic
 * con la configuración por defecto en vez de la indicada.
 * 
 * @author fjtorres
 *
 */
public class TopicCreator {

	public static void createTopics(String bootstrapServers, String... topics) throws InterruptedException, ExecutionException {

		if (bootstrapServers == null || bootstrapServers.trim().length() == 0) {
			throw new IllegalArgumentException("bootstrapServers parameter is required.");
		}

		if (topics == null || topics.length == 0) {
			return;
		}

		final Properties props = new Properties();
		props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

		final AdminClient client = AdminClient.create(props);
		
		final Set<String> existingsTopics = client.listTopics().names().get();
		
		final Collection<NewTopic> newTopics = Stream.of(topics)
				.filter(Objects::nonNull)
				.filter(t -> !existingsTopics.contains(t)) // Remove existing topics to avoid error in creation.
				.map(t -> new NewTopic(t, 3, (short) 1))
				.collect(Collectors.toList());

		// Wait for all futures instead of sleep.
		client.createTopics(newTopics).all().get();
		//Thread.sleep(5000);
	}
}
