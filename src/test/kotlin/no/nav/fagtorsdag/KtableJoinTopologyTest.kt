package no.nav.fagtorsdag

import io.kotest.core.spec.style.FreeSpec
import io.kotest.matchers.shouldBe
import java.time.Instant

class KtableJoinTopologyTest : FreeSpec({
    with(TestContext(ktableJoinTopology())) {
        "Test av ktable-join topology" - {
            "Når vi sender inn 2 tall også en tekst for samme nøkkel" {
                tallTopic.pipeInput("A", 1L)
                tallTopic.pipeInput("A", 2L)
                tekstTopic.pipeInput("A", "[A] Verdi:")
                val resultat = resultatTopic.readKeyValuesToList()
                resultat.size shouldBe 1
                resultat.first().key shouldBe "A"
                resultat.first().value shouldBe "[A] Verdi: 2"
            }

            "Når vi sender inn en tekst også et tall for samme nøkkel" {
                tekstTopic.pipeInput("B", "[B] Verdi:")
                tallTopic.pipeInput("B", 3L)
                resultatTopic.isEmpty shouldBe true
            }

            "Når vi sender inn et tall og en tekst for forskjellige nøkler" {
                tallTopic.pipeInput("C", 4L)
                tekstTopic.pipeInput("D", "[D] Verdi:")
                resultatTopic.isEmpty shouldBe true
            }

            "Når vi sender inn et tall også to tekster for samme nøkkel" {
                tallTopic.pipeInput("E", 5L)
                tekstTopic.pipeInput("E", "[E] Tekst1:")
                tekstTopic.pipeInput("E", "[E] Tekst2:")
                val resultat = resultatTopic.readKeyValuesToList()
                resultat.size shouldBe 2
                resultat.first().key shouldBe "E"
                resultat.first().value shouldBe "[E] Tekst1: 5"
                resultat[1].key shouldBe "E"
                resultat[1].value shouldBe "[E] Tekst2: 5"
            }

            "Når vi sletter tallet før vi sender inn en tekst" {
                tallTopic.pipeInput("F", 6L)
                tallTopic.pipeInput("F", null)
                tekstTopic.pipeInput("F", "[F] Verdi:")
                resultatTopic.isEmpty shouldBe true
            }
            "Når flere meldinger sendes inn ved forskjelligr tidspunkt" {
                val startTid = Instant.now()
                tallTopic.pipeInput("G", 20L, startTid)
                println("Sendte 20L")
                tallTopic.pipeInput("G", 40L, startTid.plusSeconds(30))
                println("Sendte 40L")
                tekstTopic.pipeInput("G", "[G] Verdi:", startTid.plusSeconds(1))
                println("Sendte [G] Verdi:")
                val resultat = resultatTopic.readKeyValuesToList()
                resultat.size shouldBe 1
                resultat.first().key shouldBe "G"
                resultat.first().value shouldBe "[G] Verdi: 20" //Denne feiler siden testDriver leser fortløpende
            }
        }
    }
})
