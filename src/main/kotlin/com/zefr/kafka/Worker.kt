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

        // Construct a `KStream` from the input topic "TextLinesTopic", where message values
        // represent lines of text (for the sake of this example, we ignore whatever may be stored
        // in the message keys).
        //
        // Note: We could also just call `builder.stream("TextLinesTopic")` if we wanted to leverage
        // the default serdes specified in the Streams configuration above, because these defaults
        // match what's in the actual topic.  However we explicitly set the deserializers in the
        // call to `stream()` below in order to show how that's done, too.
        //TODO: Figure out the type casting here
        topology.apply(builder)

        // Now that we have finished the definition of the processing topology we can actually run
        // it via `start()`.  The Streams application as a whole can be launched just like any
        // normal Java application that has a `main()` method.
        streams = KafkaStreams(builder.build(), kafkaConfig.kstreams.streamsConfiguration())
        // Always (and unconditionally) clean local state prior to starting the processing topology.
        // We opt for this unconditional call here because this will make it easier for you to play around with the example
        // when resetting the application for doing a re-run (via the Application Reset Tool,
        // http://docs.confluent.io/current/streams/developer-guide.html#application-reset-tool).
        //
        // The drawback of cleaning up local state prior is that your app must rebuilt its local state from scratch, which
        // will take time and will require reading all the state-relevant data from the Kafka cluster over the network.
        // Thus in a production scenario you typically do not want to clean up always as we do here but rather only when it
        // is truly needed, i.e., only under certain conditions (e.g., the presence of a command line flag for your app).
        // See `ApplicationResetExample.java` for a production-like example.
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