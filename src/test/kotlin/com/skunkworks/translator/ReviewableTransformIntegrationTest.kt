/*
 * Copyright Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.skunkworks.translator

import com.zefr.avro.message.video.ReviewableMessage
import com.zefr.avro.message.video.Video
import com.zefr.avro.message.video.VideoMessage
import io.confluent.examples.streams.IntegrationTestUtils
import io.confluent.examples.streams.kafka.EmbeddedSingleNodeKafkaCluster
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig
import io.confluent.kafka.serializers.KafkaAvroDeserializer
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig
import io.confluent.kafka.serializers.KafkaAvroSerializer
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.*
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import org.junit.BeforeClass
import org.junit.ClassRule
import org.junit.Test

import java.util.Properties

import org.junit.Assert.assertEquals
import org.junit.runner.RunWith
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.junit4.SpringRunner

/**
 * End-to-end integration test that demonstrates how to work on commons-avro-data
 *
 * This class is only minorly modified from the SpecificAvroIntegrationTest
 *
 */
@RunWith(SpringRunner::class)
@SpringBootTest()
@EnableConfigurationProperties
class ReviewableTransformIntegrationTest {

    @Autowired
    lateinit var kafkaConfig: KafkaConfig;
    @Autowired
    lateinit var reviewableTransform: ReviewableTransform

    @Test
    @Throws(Exception::class)
    fun shouldRoundTripSpecificAvroDataThroughKafka() {


        // override the schema regisry url
        kafkaConfig.schemaRegistryUrl = CLUSTER.schemaRegistryUrl()
        kafkaConfig.injectSchemaRegistryURL()


        //
        // Step 1: Configure and start the processor topology.
        //
        val builder = StreamsBuilder()

        // make a copy of the configs and set up the kafka and offset configs
        val streamsConfiguration = kafkaConfig.kstreams().streamsConfiguration();
        streamsConfiguration[StreamsConfig.BOOTSTRAP_SERVERS_CONFIG] = CLUSTER.bootstrapServers()
        streamsConfiguration[ConsumerConfig.AUTO_OFFSET_RESET_CONFIG] = "earliest"

        // apply the topology. Note that the Worker performs the same steps
        reviewableTransform.apply(builder);

        val streams = KafkaStreams(builder.build(), streamsConfiguration)
        streams.start()

        //
        // Step 2: Produce some input data to the input topic.
        //
        val inputValues: List<KeyValue<String, VideoMessage>> = listOf(KeyValue("yt1", VideoMessage.newBuilder().setPayload(Video.newBuilder().setTitle("My Awesome video").build()).build()),
                KeyValue("yt2", VideoMessage.newBuilder().setPayload(Video.newBuilder().setTitle("Your Awesome video").build()).build()))

        val producerConfig = Properties()
        producerConfig[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = CLUSTER.bootstrapServers()
        producerConfig[ProducerConfig.ACKS_CONFIG] = "all"
        producerConfig[ProducerConfig.RETRIES_CONFIG] = 0
        producerConfig[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java
        producerConfig[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = KafkaAvroSerializer::class.java
        producerConfig[AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG] = CLUSTER.schemaRegistryUrl()
        IntegrationTestUtils.produceKeyValuesSynchronously(kafkaConfig.inputTopic().name, inputValues, producerConfig)

        //
        // Step 3: Verify the application's output data.
        //
        val consumerConfig = Properties()
        consumerConfig[ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG] = CLUSTER.bootstrapServers()
        consumerConfig[ConsumerConfig.GROUP_ID_CONFIG] = "specific-avro-integration-test-standard-consumer"
        consumerConfig[ConsumerConfig.AUTO_OFFSET_RESET_CONFIG] = "earliest"
        consumerConfig[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java
        consumerConfig[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] = KafkaAvroDeserializer::class.java
        consumerConfig[AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG] = CLUSTER.schemaRegistryUrl()
        consumerConfig[KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG] = true

        // fetch all the messages we expect to receive. Adding type here to be clear
        val actualValues: List<KeyValue<String, ReviewableMessage>> = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived<String, ReviewableMessage>(consumerConfig,
                kafkaConfig.outputTopic().name, inputValues.size)
        streams.close()

        // check results
        assertEquals(2, actualValues.size)
        for (i in 0..inputValues.size-1) {
            assertEquals("Key for ${i} did not match",inputValues[i].key, actualValues[i].key)
            assertEquals("Metadata for ${i} did not match", inputValues[i].value.getMetadata(), actualValues[i].value.getMetadata())
            assertEquals("Payload for ${i} did not match", inputValues[i].value.getPayload(), actualValues[i].value.getPayload().getVideo())
        }
    }

    companion object {

        @JvmField
        @ClassRule
        val CLUSTER = EmbeddedSingleNodeKafkaCluster()

        private val inputTopic = "inputTopic2"
        private val outputTopic = "outputTopic2"

        @BeforeClass
        @JvmStatic
        @Throws(Exception::class)
        fun startKafkaCluster() {
            CLUSTER.createTopic(inputTopic)
            CLUSTER.createTopic(outputTopic)
        }
    }

}
