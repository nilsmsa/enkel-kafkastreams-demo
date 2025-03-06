package no.nav.fagtorsdag

import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.kstream.Branched
import org.apache.kafka.streams.kstream.Consumed
import org.apache.kafka.streams.kstream.Materialized
import org.apache.kafka.streams.kstream.Produced
import org.apache.kafka.streams.processor.RecordContext
import org.apache.kafka.streams.processor.TopicNameExtractor

fun splitTopology(builder: StreamsBuilder = StreamsBuilder()): Topology {
    val stream = builder
        .stream(TALL_TOPIC, Consumed.with(Serdes.String(), Serdes.Long()))
        .mapValues { _, value -> if (value > 10L) value shr 1 else value }

    stream
        .filter { _, value -> value > 10L }
        .to(TALL_TOPIC, Produced.with(Serdes.String(), Serdes.Long()))

    stream
        .filter { _, value -> value <= 10L || value == 40L}
        .mapValues { _, value -> value.toString() }
        .to(RESULTAT_TOPIC)

    return builder.build()
}