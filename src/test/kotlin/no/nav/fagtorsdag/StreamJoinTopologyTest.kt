package no.nav.fagtorsdag

import io.kotest.core.spec.style.FreeSpec
import io.kotest.matchers.shouldBe
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.TopologyTestDriver
import java.time.Duration
import java.time.Instant
import java.util.*

class StreamJoinTopologyTest : FreeSpec({
    with(TestContext(streamJoinTopology())) {
        "Test av ktable-join topology" - {
            "Når vi sender inn 2 tall også en tekst for samme nøkkel" {
                tallTopic.pipeInput("A", 1L)
                tallTopic.pipeInput("A", 2L)
                tekstTopic.pipeInput("A", "[A] Verdi:")
                val resultat = resultatTopic.readKeyValuesToList()
                resultat.size shouldBe 2
                resultat.first().key shouldBe "A"
                resultat.first().value shouldBe "[A] Verdi: 1"
                resultat[1].key shouldBe "A"
                resultat[1].value shouldBe "[A] Verdi: 2"
            }

            "Når vi sender inn 3 tall med 3 sekunders mellomrom mellom det første og de andre også en tekst for samme nøkkel" {
                val tidTall1 = Instant.now()
                val tidTall2 = tidTall1 + Duration.ofSeconds(3)
                val tidTall3 = tidTall2 + Duration.ofMillis(10)
                val tidTekst = tidTall3 + Duration.ofMillis(50)
                tallTopic.pipeInput("B", 1L, tidTall1)
                tallTopic.pipeInput("B", 2L, tidTall2)
                tallTopic.pipeInput("B", 3L, tidTall3)
                tekstTopic.pipeInput("B", "[B] Verdi:", tidTekst)
                val resultat = resultatTopic.readKeyValuesToList()
                resultat.size shouldBe 2
                resultat.first().key shouldBe "B"
                resultat.first().value shouldBe "[B] Verdi: 2"
                resultat[1].key shouldBe "B"
                resultat[1].value shouldBe "[B] Verdi: 3"
            }

            "Når vi sender inn en tekst også et tall for samme nøkkel" {
                tekstTopic.pipeInput("C", "[C] Verdi:")
                tallTopic.pipeInput("C", 3L)
                resultatTopic.isEmpty shouldBe true
            }
        }
    }
})
