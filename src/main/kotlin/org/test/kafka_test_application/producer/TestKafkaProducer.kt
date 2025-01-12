package org.test.kafka_test_application.producer

import org.slf4j.LoggerFactory
import org.springframework.http.ResponseEntity
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.bind.annotation.RestController
import org.test.kafka_test_application.dto.TopicInfo

@RestController
@RequestMapping("/api/producer")
class TestKafkaProducer(
    private val kafkaTemplate: KafkaTemplate<String, String>
) {

    companion object{
        private val logger = LoggerFactory.getLogger(TestKafkaProducer::class.java)
    }


    @GetMapping("/send")
    fun send(
        @RequestParam("key") key: String,
        @RequestParam("value") value: String
    ): ResponseEntity<Int> {


        runCatching {

            //val rand = (0..1).random()

            val sendMessage = kafkaTemplate.send("listener-test", key, value)

          sendMessage.join()

           // val record = sendResult.producerRecord

           // logger.info("Sent message: key=${record.key()}, value=${record.value()}")

        }.fold(
            onSuccess = {
                return ResponseEntity.ok(200)
            },
            onFailure = { e ->
                return ResponseEntity.internalServerError().body(500)
            }
        )
    }
}