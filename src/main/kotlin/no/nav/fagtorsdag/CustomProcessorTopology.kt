package no.nav.fagtorsdag

import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.kstream.Consumed
import org.apache.kafka.streams.processor.api.Processor
import org.apache.kafka.streams.processor.api.ProcessorContext
import org.apache.kafka.streams.processor.api.ProcessorSupplier
import org.apache.kafka.streams.processor.api.Record
import org.apache.kafka.streams.state.KeyValueStore
import java.time.Instant

fun customProcessorTopology(
    builder: StreamsBuilder = StreamsBuilder(),
    stateStoreName: String
): Topology {
    builder
        .stream(TALL_TOPIC, Consumed.with(Serdes.String(), Serdes.Long()))
        .process(
            ProcessorSupplier {
                LagringsProcessor(stateStoreName)
            },
            stateStoreName
        )

    builder
        .stream<String, String>(TEKST_TOPIC)
        .process(
            ProcessorSupplier { TekstTallJoinProcessor(stateStoreName) },
            stateStoreName
        ).to(RESULTAT_TOPIC)

    return builder.build()
}

class TekstTallJoinProcessor(private val stateStoreName: String) : Processor<String, String, String, String> {
    private var cotext: ProcessorContext<String, String>? = null
    private var stateStore: KeyValueStore<String, String>? = null

    override fun init(context: ProcessorContext<String, String>?) {
        super.init(context)
        this.cotext = context
        this.stateStore = context?.getStateStore(stateStoreName)
    }

    override fun process(record: Record<String, String?>?) {
        val stateStore = requireNotNull(stateStore)
        val ctx = requireNotNull(cotext)
        record?.let { process(ctx, stateStore, it) }
    }

    private fun process(
        context: ProcessorContext<String, String>,
        store: KeyValueStore<String, String>,
        record: Record<String, String?>
    ) {
        val lagretVerdi: String? = store.get(record.key())
        val somListe = lagretVerdi?.split(",") ?: emptyList()
        val sum = somListe.fold(0) { sum, l -> sum + l.toInt() }
        context.forward(record.withValue("${record.value()} $sum"))
    }
}

class LagringsProcessor(private val stateStoreName: String) : Processor<String, Long, String, String> {
    private var cotext: ProcessorContext<String, String>? = null
    private var stateStore: KeyValueStore<String, String>? = null

    override fun init(context: ProcessorContext<String, String>?) {
        super.init(context)
        this.cotext = context
        this.stateStore = context?.getStateStore(stateStoreName)
    }

    override fun process(record: Record<String, Long?>?) {
        val stateStore = requireNotNull(stateStore)
        record?.let { process(stateStore, it) }
    }

    private fun process(
        store: KeyValueStore<String, String>,
        record: Record<String, Long?>
    ) {
        println("Record: $record")
        println("Streamtime: ${Instant.ofEpochMilli(cotext!!.currentStreamTimeMs())}")
        println("Wallclock: ${Instant.ofEpochMilli(cotext!!.currentSystemTimeMs())}")
        val value = record.value()
        if (value == null) {
            store.delete(record.key())
            return
        } else {
            val lagretVerdi: String? = store.get(record.key())
            val somListe = lagretVerdi?.split(",") ?: emptyList()
            val tmpVerdi = somListe + value.toString()
            val nyVerdi = if (tmpVerdi.size > 4) {
                tmpVerdi.subList(1, tmpVerdi.size)
            } else {
                tmpVerdi
            }
            store.put(record.key(), nyVerdi.joinToString(","))
        }
    }
}
