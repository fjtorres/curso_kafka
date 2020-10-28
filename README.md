# Test project from Kafka course

This project contains different samples to use Kafka via Java API.

# Information about kafka

## General


## Topics
- The topics contains the messages as log changes, in a file.
- The topis are compossed by partitions, at least one.
- The messages are compossed by key and value.
- Messages are distributed in partitions, the distribution is done by an algorithm.
- The algorithm coulbd round robin, murmur 3 or custom (implementing specific class). These algorithms will determine where each message will be located (in partitions) using the key as input. So same key will be always in the same partition.


## Partitions
- One partition is managed by single broker but will be replicated to other brokers depends on replication factor. So one of the brokers will have the partition leader, where the messages will be received. The changes on the partition will be replicated to other brokers automatically.
- If a broker go down the cluster will select another partition leader automatically.

## Replicas
- The maximum value for the replication factor should equals to amount of brokers.
- Each replica will be syncronized automatically.

## Producer
- Responsible to send messages to topics.
- Could send messages to specific partition by configuration options.
- It's connected to partition leader to use the right broker to send messages.

## Consumer
- Responsible to receive or process messages from topics.
- It's groupued by group id, a message only will be processed by single consumer from same group.
- Could read message from specific partition by configuration options.
- An internal topic called "__consumer_offsets" save the information about the offset for each consumer group and partition. Last operation confirmed to server.
- Every connection or disconnection of a consumer generate a recalculation of the assigned partions to each consumer.
- You can autocommit the process of message or do it manually, however if you do it manually could be dangerous because a message could be processed twice if the confirmation never reach the server.
- If we have more consumers than partitions, we will have consumers waiting to work. This will happen when a recalculation of the asigned partions occurs. So we should not have more consumers than partitions or at least not too much to avoid unnecessary nodes in servers.
