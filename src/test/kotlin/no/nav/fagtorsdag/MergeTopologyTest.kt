package no.nav.fagtorsdag

import io.kotest.core.spec.style.FreeSpec
import io.kotest.matchers.shouldBe
import no.nav.fagtorsdag.utils.TestContext
import java.time.Instant

class MergeTopologyTest : FreeSpec({
    with(TestContext(mergeTopology())) {
        "Test av ktable-join topology" - {
            "Når vi sender inn 2 tall også en tekst for samme nøkkel" {
                tallTopic.pipeInput("A", 1L)
                tallTopic.pipeInput("A", 2L)
                tekstTopic.pipeInput("A", "3")
                val resultat = resultatTopic.readKeyValuesToList()
                resultat.size shouldBe 3
                resultat.first().key shouldBe "A"
                resultat.first().value shouldBe "1"
                resultat[1].key shouldBe "A"
                resultat[1].value shouldBe "2"
                resultat[2].key shouldBe "A"
                resultat[2].value shouldBe "3"
            }

            "Når vi sender inn med timestamp" {
                val startTid = Instant.now()
                tallTopic.pipeInput("G", 20L, startTid)
                println("Sendte 20L")
                tallTopic.pipeInput("G", 40L, startTid.plusSeconds(30))
                println("Sendte 40L")
                tekstTopic.pipeInput("G", "1", startTid.plusSeconds(1))
                println("Sendte 1(String)")
                val resultat = resultatTopic.readKeyValuesToList()
                resultat.size shouldBe 3
                resultat.first().key shouldBe "G"
                resultat.first().value shouldBe "20"
                resultat[1].key shouldBe "G"
                resultat[1].value shouldBe "40"
                resultat[2].key shouldBe "G"
                resultat[2].value shouldBe "1"
            }
        }
    }
})
