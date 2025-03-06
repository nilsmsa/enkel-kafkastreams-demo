package no.nav.fagtorsdag

import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.kstream.Consumed
import org.apache.kafka.streams.processor.PunctuationType
import org.apache.kafka.streams.processor.api.Processor
import org.apache.kafka.streams.processor.api.ProcessorContext
import org.apache.kafka.streams.processor.api.ProcessorSupplier
import org.apache.kafka.streams.processor.api.Record
import org.apache.kafka.streams.state.KeyValueStore
import java.time.Duration
import java.time.Instant

fun scheduledProcessorToplogy(
    builder: StreamsBuilder = StreamsBuilder(),
    stateStoreName: String,
    punctuationType: PunctuationType,
): Topology {
    builder
        .stream(TALL_TOPIC, Consumed.with(Serdes.String(), Serdes.Long()))
        .process(
            ProcessorSupplier {
                LagringsProcessor(stateStoreName)
            },
            stateStoreName
        ).process(
            ProcessorSupplier { StørsteVerdiSisteIntervall(punctuationType, stateStoreName) },
            stateStoreName
        ).mapValues { _, value -> value.toString() }
        .to(RESULTAT_TOPIC)


    return builder.build()
}

class StørsteVerdiSisteIntervall(
    private val punctuationType: PunctuationType,
    private val stateStoreName: String
) : Processor<String, String, String, Int> {
    override fun init(context: ProcessorContext<String, Int>?) {
        super.init(context)
        context?.schedule(
            Duration.ofSeconds(30),
            punctuationType
        ) { timestamp ->
            println("Kjører jobb: ${Instant.ofEpochMilli(timestamp)}")
            val keyvalueStore: KeyValueStore<String, String> = context.getStateStore(stateStoreName)
            keyvalueStore.all().use { iterator ->
                iterator.forEach { keyvalue ->
                    val key = keyvalue.key
                    keyvalue.value
                        .split(",")
                        .map { it.toInt() }
                        .maxOrNull()
                        ?.let { max ->
                            context.forward(Record(key, max, timestamp))
                            keyvalueStore.delete(key)
                        }
                }
            }
        }

    }

    override fun process(record: Record<String, String?>?) {}

}

