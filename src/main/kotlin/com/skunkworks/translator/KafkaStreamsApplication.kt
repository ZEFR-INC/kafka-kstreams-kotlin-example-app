package com.skunkworks.translator

import org.springframework.boot.SpringApplication
import org.springframework.boot.autoconfigure.SpringBootApplication

@SpringBootApplication
open class KafkaStreamsApplication {

    companion object {

        @JvmStatic fun main(args: Array<String>) {
            SpringApplication.run(KafkaStreamsApplication::class.java, *args)
        }
    }


}