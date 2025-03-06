package no.nav.fagtorsdag

import io.kotest.core.spec.style.FreeSpec
import io.kotest.matchers.shouldBe
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.common.utils.Time
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.state.internals.InMemoryKeyValueBytesStoreSupplier
import org.apache.kafka.streams.state.internals.KeyValueStoreBuilder

class CustomProcessorTopologyTest : FreeSpec({
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
    with(TestContext(customProcessorTopology(builder, storeName))) {
        "Test av custom processor topology" - {
            "Når vi sender inn 2 tall også en tekst for samme nøkkel" {
                tallTopic.pipeInput("A", 1L)
                tallTopic.pipeInput("A", 2L)
                tekstTopic.pipeInput("A", "[A] Verdi:")
                val resultat = resultatTopic.readKeyValuesToList()
                resultat.size shouldBe 1
                resultat.first().key shouldBe "A"
                resultat.first().value shouldBe "[A] Verdi: 3"
            }
            "Når vi sender inn 6 tall også en tekst for samme nøkkel" {
                tallTopic.pipeInput("A", 1L)
                tallTopic.pipeInput("A", 2L)
                tallTopic.pipeInput("A", 2L)
                tallTopic.pipeInput("A", 2L)
                tallTopic.pipeInput("A", 10L)
                tallTopic.pipeInput("A", 100L)
                tekstTopic.pipeInput("A", "[A] Verdi:")
                val resultat = resultatTopic.readKeyValuesToList()
                resultat.size shouldBe 1
                resultat.first().key shouldBe "A"
                resultat.first().value shouldBe "[A] Verdi: 114"
            }
        }
    }
})
