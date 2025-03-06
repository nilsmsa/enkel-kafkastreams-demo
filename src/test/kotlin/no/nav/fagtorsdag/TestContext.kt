package no.nav.fagtorsdag

import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.TopologyTestDriver
import java.time.Instant
import java.util.*

class TestContext(
    topology: Topology,
    initialWallclock: Instant = Instant.now()
) {
    val testDriver = TopologyTestDriver(
        topology,
        Properties().apply {
            set(StreamsConfig.APPLICATION_ID_CONFIG, UUID.randomUUID().toString())
            set(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String()::class.java)
            set(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String()::class.java)
        },
        initialWallclock
    )

    val tallTopic = testDriver.createInputTopic(
        TALL_TOPIC,
        Serdes.String().serializer(),
        Serdes.Long().serializer(),
    )
    val tekstTopic = testDriver.createInputTopic(
        TEKST_TOPIC,
        Serdes.String().serializer(),
        Serdes.String().serializer()
    )
    val resultatTopic = testDriver.createOutputTopic(
        RESULTAT_TOPIC,
        Serdes.String().deserializer(),
        Serdes.String().deserializer()
    )
}