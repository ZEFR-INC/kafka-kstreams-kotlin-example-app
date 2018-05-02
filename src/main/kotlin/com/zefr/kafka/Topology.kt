package com.zefr.kafka

import org.apache.kafka.streams.StreamsBuilder

interface Topology {
    fun apply(builder: StreamsBuilder)
}