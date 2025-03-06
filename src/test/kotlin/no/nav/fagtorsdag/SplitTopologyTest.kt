package no.nav.fagtorsdag

import io.kotest.core.spec.style.FreeSpec
import io.kotest.matchers.shouldBe
import java.time.Instant

class SplitTopologyTest : FreeSpec({
    with(TestContext(splitTopology())) {
        "Test av ktable-join topology" - {
            "Når vi sender inn 10 får vi ut 10" {
                tallTopic.pipeInput("A", 10L)
                val resultat = resultatTopic.readKeyValuesToList()
                resultat.size shouldBe 1
                resultat.first().key shouldBe "A"
                resultat.first().value shouldBe "10"
            }

            "Når vi sender inn 80 får vi ut 40 og 10" {
                tallTopic.pipeInput("A", 80L)
                val resultat = resultatTopic.readKeyValuesToList()
                resultat.size shouldBe 2
                resultat.first().key shouldBe "A"
                resultat.first().value shouldBe "40"
                resultat[1].key shouldBe "A"
                resultat[1].value shouldBe "10"
            }
        }
    }
})
