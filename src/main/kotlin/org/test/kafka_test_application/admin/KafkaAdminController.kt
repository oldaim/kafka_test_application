package org.test.kafka_test_application.admin

import org.apache.kafka.clients.admin.AdminClient
import org.slf4j.LoggerFactory
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.*
import org.test.kafka_test_application.dto.TopicInfo

@RestController
@RequestMapping("/api/admin")
class KafkaAdminController(
    private val adminClient: AdminClient
) {

    companion object{
        private val logger = LoggerFactory.getLogger(KafkaAdminController::class.java)
    }

    @PostMapping("/create-topic")
    fun createTopics(
        @RequestBody topicInfos: List<TopicInfo>
    ): ResponseEntity<Int> {

    return runCatching {
        topicInfos.forEach {

        }
        val newTopics = topicInfos.map { TopicInfo.toNewTopic(it) }
        adminClient.createTopics(newTopics).all().get()
    }.fold(
        onSuccess = {
            ResponseEntity.ok(200)
        },
        onFailure = { e ->
            ResponseEntity.internalServerError().body(500)
        }
    )

    }

}