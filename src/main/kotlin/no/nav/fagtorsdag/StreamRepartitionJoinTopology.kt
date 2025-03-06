package no.nav.fagtorsdag

import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.kstream.Consumed
import org.apache.kafka.streams.kstream.JoinWindows
import org.apache.kafka.streams.kstream.Repartitioned
import java.time.Duration

fun streamRepartitionJoinTopology(builder: StreamsBuilder = StreamsBuilder()): Topology {
    val tallStrøm = builder
        .stream(TALL_TOPIC, Consumed.with(Serdes.String(), Serdes.Long()))
        .map { key, value -> KeyValue(key.split("-")[0], value) }
        .repartition(Repartitioned.with(Serdes.String(), Serdes.Long()))
        .mapValues { _, value -> value.toString() }

    builder
        .stream<String, String>(TEKST_TOPIC)
        .join(
            tallStrøm,
            { tekst, tall -> "$tekst $tall" },
            JoinWindows.ofTimeDifferenceAndGrace(
                Duration.ofMillis(1000),
                Duration.ofMillis(1000)
            )
        ).to(RESULTAT_TOPIC)
    return builder.build()
}