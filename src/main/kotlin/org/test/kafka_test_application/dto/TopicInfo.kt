package org.test.kafka_test_application.dto

import org.apache.kafka.clients.admin.NewTopic

data class TopicInfo(
    val topicName: String,
    val numPartitions: Int,
    val replicationFactor: Short
){
    companion object{
        fun toNewTopic(topicInfo: TopicInfo): NewTopic{
            return NewTopic(topicInfo.topicName, topicInfo.numPartitions, topicInfo.replicationFactor)
        }
    }
}
