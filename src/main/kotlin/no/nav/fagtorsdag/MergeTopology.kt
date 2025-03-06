package no.nav.fagtorsdag

import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.kstream.Consumed

fun mergeTopology(builder: StreamsBuilder = StreamsBuilder()): Topology {
    val strøm1 = builder
        .stream(TALL_TOPIC, Consumed.with(Serdes.String(), Serdes.Long()))
        .mapValues { _, value -> value.toString() }
        .peek { _, value -> println("[${TALL_TOPIC}] $value") }

    builder
        .stream<String, String>(TEKST_TOPIC)
        .peek { _, value -> println("[${TEKST_TOPIC}] $value") }
        .merge(strøm1)
        .to(RESULTAT_TOPIC)

    return builder.build()
}