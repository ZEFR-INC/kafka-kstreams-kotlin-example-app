package com.skunkworks.translator

import org.apache.kafka.streams.StreamsBuilder

interface Topology {
    fun apply(builder: StreamsBuilder)
}