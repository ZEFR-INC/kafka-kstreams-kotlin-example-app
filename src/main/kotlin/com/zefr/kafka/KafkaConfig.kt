package com.zefr.kafka

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde
import org.apache.avro.specific.SpecificRecord
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.StreamsConfig
import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.context.annotation.PropertySource
import org.springframework.stereotype.Component
import java.util.*
import javax.annotation.PostConstruct

/**
 * Class that builds Google config objects from the
 * application properties file. The object values can be overridden
 * by setting tham as environment variables
 */
@Component
@PropertySource("classpath:application.properties")
@ConfigurationProperties("app.kafka")
class KafkaConfig<K, V, K2, V2> {

    val kstreams: KStreamsConfig = KStreamsConfig()
    val inputTopic: KafkaTopicConfig<K,V> = KafkaTopicConfig()
    val outputTopic: KafkaTopicConfig<K2,V2> = KafkaTopicConfig()
    lateinit var schemaRegistryUrl: String

    //TODO: This null bug is an issue with Spring and kotlin. Super annoying.
    // A data class version of this would be correct
    // https://github.com/spring-projects/spring-boot/issues/8762

    @PostConstruct
    fun injectSchemaRegistryURL() {
        kstreams.schemaRegristryUrl = schemaRegistryUrl
        inputTopic.schemaRegistryUrl = schemaRegistryUrl
        outputTopic.schemaRegistryUrl = schemaRegistryUrl
    }



    class KStreamsConfig {
        lateinit var bootstrapServers: String
        lateinit var applicationId: String
        lateinit var clientId: String
        lateinit var schemaRegristryUrl: String
        var keySerdeClass: String = Serdes.String().javaClass.name
        var valueSerdeClass: String = SpecificAvroSerde<SpecificRecord>().javaClass.name
        var commitIntervalMs: Int = 10 * 1000 // 10 seconds
        var doCleanUp: Boolean = false
        var doStart: Boolean = true

        fun streamsConfiguration(): Properties {
            val streamsConfiguration = Properties()
            // Give the Streams application a unique name.  The name must be unique in the Kafka cluster
            // against which the application is run.
            streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId)
            streamsConfiguration.put(StreamsConfig.CLIENT_ID_CONFIG, clientId)
            // Where to find Kafka broker(s).
            streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
            // Specify default (de)serializers for record keys and for record values.
            streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, keySerdeClass)
            streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, valueSerdeClass)
            // Records should be flushed every 10 seconds. This is less than the default
            // in order to keep this example interactive.
            streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, commitIntervalMs)
            streamsConfiguration.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegristryUrl);
            return streamsConfiguration
        }

    }

    class KafkaTopicConfig<KEY, VALUE> {
        lateinit var name: String
        var keySerdeClass: String = Serdes.String().javaClass.name
        var valueSerdeClass: String = SpecificAvroSerde<SpecificRecord>().javaClass.name
        lateinit var schemaRegistryUrl: String
        fun keySerde(): Serde<KEY> = constructSerde(keySerdeClass, schemaRegistryUrl, true) as Serde<KEY>
        fun valueSerde(): Serde<VALUE> = constructSerde(valueSerdeClass, schemaRegistryUrl, false) as Serde<VALUE>
    }

    companion object {
        fun constructSerde(serdeName: String, schemaRegristryUrl: String, keySerde: Boolean): Serde<Any> {
            val kClass = Class.forName(serdeName).kotlin
            val serde: Serde<Any> = kClass.constructors
                    .first({ t ->
                        t.parameters.isEmpty()
                    }).call() as Serde<Any>;
            if (serdeName.contains(SpecificAvroSerde<SpecificRecord>().javaClass.name)) {
                serde.configure( mapOf(
                        "schema.registry.url" to schemaRegristryUrl),
                        keySerde)
            }
            return serde;
        }
    }


}
