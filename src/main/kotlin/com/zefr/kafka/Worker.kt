package com.zefr.kafka

import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsBuilder
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Component
import javax.annotation.PostConstruct
import javax.annotation.PreDestroy


/**
 * @author jkorab
 */
@Component("worker")
class Worker @Autowired constructor(
        private val kafkaConfig: KafkaConfig<Any,Any,Any,Any>,
        private val topology: Topology
){

    private lateinit var streams: KafkaStreams


    @PostConstruct
    fun run() {

        // In the subsequent lines we define the processing topology of the Streams application.
        val builder = StreamsBuilder()

        // Construct a `KStream` from the input topic
        // The apply method does all the heavy lifting in terms of configuring the input and output topic and handling
        // the business logic
        topology.apply(builder)

        // Now that we have finished the definition of the processing topology we can actually run
        // it via `start()`.
        streams = KafkaStreams(builder.build(), kafkaConfig.kstreams.streamsConfiguration())
        // Clean local state prior to starting the processing topology. This  will make it easier for you to play around with the example
        // when resetting the application for doing a re-run (via the Application Reset Tool,
        // http://docs.confluent.io/current/streams/developer-guide.html#application-reset-tool).
        //
        // The drawback of cleaning up local state prior is that your app must rebuilt its local state from scratch, which
        // will take time and will require reading all the state-relevant data from the Kafka cluster over the network.
        // Thus in a production scenario you typically do not want to clean up but rather only when it
        // is truly needed, i.e., only under certain conditions (e.g.,setting app.kafka.kstreams.doCleanUp to true at trun time).
        if (kafkaConfig.kstreams.doCleanUp)
            streams.cleanUp()

        // disable for unit tests only
        if (kafkaConfig.kstreams.doStart)
            streams.start()

    }


    @PreDestroy
    fun closeStream() {
        if (kafkaConfig.kstreams.doStart)
            streams.close()
    }
}