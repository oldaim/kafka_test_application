package org.test

import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import java.util.*

fun main() {

    val topicName = "test-topic"
    
    val brokerUrl = "http://localhost:9092"

    creatTopic(topicName = topicName, brokerUrl = brokerUrl)

    val producer = createProducer(brokerUrl = brokerUrl)

    val record = ProducerRecord(topicName, "tests", "Hello, Kafka!")

   producer.send(record) { metadata, exception ->
        if (exception != null) {
            println("Failed to send record: $exception")
        } else {
            println("Record sent to partition ${metadata.partition()}, offset ${metadata.offset()}")
        }
    }

    producer.flush()

    producer.close()

}

fun createProducer(brokerUrl: String): KafkaProducer<String, String> {

    val props = Properties().apply {
        put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerUrl)
        put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java)
        put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java)
        put(ProducerConfig.ACKS_CONFIG, "all")
    }

    return KafkaProducer(props)
}

fun creatTopic(topicName: String, brokerUrl: String, numPartitions: Int = 1, replicationFactor: Short = 1) {

    val props = Properties().apply {
        put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerUrl)
    }

    val adminClient = AdminClient.create(props)

    val newTopic = NewTopic(topicName, numPartitions, replicationFactor)

    adminClient.createTopics(listOf(newTopic))

    adminClient.close()
}