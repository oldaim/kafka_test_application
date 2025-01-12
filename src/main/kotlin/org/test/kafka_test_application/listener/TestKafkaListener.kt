package org.test.kafka_test_application.listener

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.annotation.TopicPartition
import org.springframework.stereotype.Component
import java.util.concurrent.atomic.AtomicInteger

@Component
class TestKafkaListener {

    val listenerCount1 = AtomicInteger(0)
    val listenerCount2 = AtomicInteger(0)

    companion object {
        private val logger = LoggerFactory.getLogger(TestKafkaListener::class.java)
    }

    @KafkaListener(
        topics = ["listener-test"],
        groupId = "test-group",
        topicPartitions = [TopicPartition(topic = "listener-test", partitions = ["0"])],
        containerFactory = "kafkaListenerContainerFactory"
    )
    fun listen(record: ConsumerRecord<String, String>) {

        logger.info("Listener1< thread: {${Thread.currentThread().name}} - count: ${listenerCount1.getAndIncrement()}>")
    }

    @KafkaListener(
        topics = ["listener-test"],
        groupId = "test-group",
        topicPartitions = [TopicPartition(topic = "listener-test", partitions = ["1"])],
        containerFactory = "kafkaListener2ContainerFactory"
    )
    fun listen2(record: ConsumerRecord<String, String>) {
        logger.info("Listener2< thread: {${Thread.currentThread().name}} count: ${listenerCount2.getAndIncrement()}>")
    }
}