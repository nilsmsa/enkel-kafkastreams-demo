package no.nav.fagtorsdag

import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.kstream.Consumed
import org.apache.kafka.streams.kstream.Materialized
import org.apache.kafka.streams.kstream.Produced

fun ktableJoinTopology(builder: StreamsBuilder = StreamsBuilder()): Topology {
    val table = builder
        .stream(TALL_TOPIC, Consumed.with(Serdes.String(), Serdes.Long()))
        .peek { key, value -> println("[$TALL_TOPIC]Leste: $key $value") }
        .toTable(Materialized.with(Serdes.String(), Serdes.Long()))

    builder
        .stream<String, String>(TEKST_TOPIC)
        .peek { key, value -> println("[$TEKST_TOPIC]Leste: $key $value") }
        .join(table) { tekst, tall -> "$tekst $tall" }
        .to(RESULTAT_TOPIC)

    return builder.build()
}