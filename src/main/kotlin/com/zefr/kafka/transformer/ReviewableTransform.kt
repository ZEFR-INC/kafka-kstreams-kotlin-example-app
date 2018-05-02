package com.zefr.kafka.transformer

import com.zefr.avro.message.video.Reviewable
import com.zefr.avro.message.video.ReviewableMessage
import com.zefr.avro.message.video.VideoMessage
import com.zefr.kafka.KafkaConfig
import com.zefr.kafka.Topology
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.Consumed
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.kstream.Produced
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Component


@Component("reviewableTransform")
class ReviewableTransform @Autowired constructor(
        private val kafkaConfig: KafkaConfig<String, VideoMessage, String, ReviewableMessage>
) : Topology {

    override fun apply(builder: StreamsBuilder) {
        val textLines = builder.stream(kafkaConfig.inputTopic.name,
                Consumed.with(kafkaConfig.inputTopic.keySerde(),
                        kafkaConfig.inputTopic.valueSerde())
        )
        textLines.mapValues { video: VideoMessage ->
            ReviewableMessage.newBuilder().setMetadata(video.getMetadata()).setPayload(
                    Reviewable.newBuilder().setVideo(video.getPayload()).build()).build()
        }.to(kafkaConfig.outputTopic.name, Produced.with(kafkaConfig.outputTopic.keySerde(),
                    kafkaConfig.outputTopic.valueSerde()))
    }


}