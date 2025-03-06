package no.nav.fagtorsdag

import io.kotest.core.spec.style.FreeSpec
import io.kotest.matchers.shouldBe
import no.nav.fagtorsdag.utils.TestContext
import no.nav.fagtorsdag.utils.sekunder
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.common.utils.Time
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.processor.PunctuationType
import org.apache.kafka.streams.state.internals.InMemoryKeyValueBytesStoreSupplier
import org.apache.kafka.streams.state.internals.KeyValueStoreBuilder
import java.time.Duration
import java.time.Instant

class ScheduledProcessorTopologyTest : FreeSpec({
    val storeName = "state-store"
    val builder = StreamsBuilder()
    builder.addStateStore(
        KeyValueStoreBuilder(
            InMemoryKeyValueBytesStoreSupplier(storeName),
            Serdes.String(),
            Serdes.String(),
            Time.SYSTEM
        )
    )
    val startTid = Instant.parse("2018-03-12T12:19:02.00Z")
    with(
        TestContext(
        initialWallclock = startTid,
        topology = scheduledProcessorToplogy(builder, storeName, PunctuationType.STREAM_TIME))
    ) {
        "Test av custom processor topology med 'stream time'" - {
            "Når vi sender inn flere tall innen 30 sekunder får vi ut det største tallet" {
                tallTopic.pipeInput("A", 10L, startTid)
                tallTopic.pipeInput("A", 2L, startTid + 2.sekunder)
                tallTopic.pipeInput("A", 5L, startTid + 4.sekunder)
                tallTopic.pipeInput("A", 6L, startTid + 29.sekunder)
                tallTopic.pipeInput("A", 3L, startTid + 20.sekunder)
                tallTopic.pipeInput("A", 1L, startTid + 25.sekunder)
                val resultat = resultatTopic.readKeyValuesToList()
                resultat.size shouldBe 2
                resultat[0].value shouldBe "10"
                resultat[1].value shouldBe "6"
            }
        }
    }
    val startTidTest2 = Instant.parse("2020-10-12T12:19:02.00Z")
    val storeName2 = "state-store2"
    val builder2 = StreamsBuilder()
    builder2.addStateStore(
        KeyValueStoreBuilder(
            InMemoryKeyValueBytesStoreSupplier(storeName2),
            Serdes.String(),
            Serdes.String(),
            Time.SYSTEM
        )
    )
    with(
        TestContext(
        initialWallclock = startTid,
        topology = scheduledProcessorToplogy(builder2, storeName2, PunctuationType.WALL_CLOCK_TIME))
    ) {
        "Test av custom processor topology med 'wall clock time'" - {
            "Når vi sender inn flere tall innen 60 sekunder" - {
                tallTopic.pipeInput("A", 5L, startTidTest2)
                tallTopic.pipeInput("A", 2L, startTidTest2 + 2.sekunder)
                tallTopic.pipeInput("A", 5L, startTidTest2 + 4.sekunder)
                tallTopic.pipeInput("A", 6L, startTidTest2 + 29.sekunder)
                tallTopic.pipeInput("A", 9L, startTidTest2 + 20.sekunder)
                tallTopic.pipeInput("A", 1L, startTidTest2 + 25.sekunder)
                tallTopic.pipeInput("A", 1L, startTidTest2 + 61.sekunder)
                "får vi ikke ut noe som helst" {
                    resultatTopic.isEmpty shouldBe true
                }
                "dersom vi kjører 'Thread.sleep' i 35 sekunder får vi ikke ut noe" {
                    Thread.sleep(35_000)
                    resultatTopic.isEmpty shouldBe true
                }
                "dersom vi setter wallclock frem 31 sekunder får vi ut '9'" {
                    testDriver.advanceWallClockTime(31.sekunder)
                    resultatTopic.isEmpty shouldBe false
                    resultatTopic.readValue() shouldBe "9"
                }
            }
        }
    }
})

