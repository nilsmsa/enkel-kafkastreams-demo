package no.nav.fagtorsdag

import io.kotest.core.spec.style.FreeSpec
import io.kotest.matchers.shouldBe
import no.nav.fagtorsdag.utils.TestContext

class StreamRepartitionJoinTopologyTest: FreeSpec({
    with(TestContext(streamRepartitionJoinTopology())) {
        "Test av ktable-join topology" - {
            "Når vi sender inn 2 tall også en tekst for samme nøkkel" {
                tallTopic.pipeInput("A-B", 1L)
                tallTopic.pipeInput("A-C", 2L)
                tekstTopic.pipeInput("A", "[A] Verdi:")
                val resultat = resultatTopic.readKeyValuesToList()
                resultat.size shouldBe 2
                resultat.first().key shouldBe "A"
                resultat.first().value shouldBe "[A] Verdi: 1"
                resultat[1].key shouldBe "A"
                resultat[1].value shouldBe "[A] Verdi: 2"
            }
        }
    }
})
