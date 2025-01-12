package org.test.kafka_test_application.config

import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.admin.KafkaAdminClient
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.annotation.EnableKafka
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory
import org.springframework.kafka.core.*
import org.springframework.kafka.support.serializer.JsonSerializer
import org.test.kafka_test_application.dto.TestJson

@Configuration
@EnableKafka
class KafkaConfig {


    @Bean
    fun consumerFactory(): ConsumerFactory<String, TestJson> {
        val props = mutableMapOf<String, Any>()
        props[ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG] = "localhost:9092"
        props[ConsumerConfig.GROUP_ID_CONFIG] = "test-group"
        props[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java
        props[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] = JsonSerializer<TestJson>().javaClass

        return DefaultKafkaConsumerFactory(props)
    }

    @Bean
    fun consumer2Factory(): ConsumerFactory<String, TestJson> {
        val props = mutableMapOf<String, Any>()
        props[ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG] = "localhost:9092"
        props[ConsumerConfig.GROUP_ID_CONFIG] = "test-group"
        props[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java
        props[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] = JsonSerializer<TestJson>().javaClass

        return DefaultKafkaConsumerFactory(props)
    }

    @Bean
    fun kafkaListenerContainerFactory(): ConcurrentKafkaListenerContainerFactory<String, TestJson> {
        val factory = ConcurrentKafkaListenerContainerFactory<String, TestJson>()
        factory.consumerFactory = consumerFactory()
        factory.setConcurrency(1)
        return factory
    }

    @Bean
    fun kafkaListener2ContainerFactory(): ConcurrentKafkaListenerContainerFactory<String, TestJson> {
        val factory = ConcurrentKafkaListenerContainerFactory<String, TestJson>()
        factory.consumerFactory = consumer2Factory()
        factory.setConcurrency(1)
        return factory
    }



    @Bean
    @ConditionalOnBean(ConsumerFactory::class)
    fun batchListenerContainerFactory(): ConcurrentKafkaListenerContainerFactory<String, TestJson> {
        val factory = ConcurrentKafkaListenerContainerFactory<String, TestJson>()
        factory.consumerFactory = consumerFactory()
        factory.isBatchListener = true
        return factory
    }

    @Bean
    fun producerFactory(): ProducerFactory<String, TestJson> {
        val props = mutableMapOf<String, Any>()
        props[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = "localhost:9092"
        props[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java
        props[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = JsonSerializer<TestJson>().javaClass

        return DefaultKafkaProducerFactory(props)
    }

    @Bean
    @ConditionalOnBean(ProducerFactory::class)
    fun kafkaTemplate(producerFactory: ProducerFactory<String, TestJson>) : KafkaTemplate<String, TestJson> {
        val kafkaTemplate = KafkaTemplate(producerFactory)

        kafkaTemplate.transactionIdPrefix = "tx-oldaim-"

        return kafkaTemplate
    }

    @Bean
    fun kafkaAdminClient(): AdminClient {

        val props = mutableMapOf<String, Any>()

        props[AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG] = "localhost:9092"

        return AdminClient.create(props)
    }

}