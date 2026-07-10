package br.ufes.edu;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import br.ufes.edu.domain.AggregatedFlow;
import br.ufes.edu.domain.RollingStats;
import br.ufes.edu.domain.Warning;
import br.ufes.edu.serdes.AggregatedFlowParser;
import br.ufes.edu.serdes.RollingStatsDeserializer;
import br.ufes.edu.serdes.RollingStatsSerializer;
import br.ufes.edu.serdes.WarningDeserializer;
import br.ufes.edu.serdes.WarningSerializer;

public class OutputWatcher {

    // ---- TÓPICOS ----
    // Mesmo tópico de saída do agregador (App.java) e de entrada do Splitter.java —
    // este componente é só mais um consumidor independente do mesmo stream.
    private static final String INPUT_TOPIC  = "network-microflows-aggregated";
    private static final String OUTPUT_TOPIC = "network-warnings";

    // ---- STATE STORES (um por métrica monitorada) ----
    private static final String STORE_INBOUND  = "rolling-inbound-store";
    private static final String STORE_OUTBOUND = "rolling-outbound-store";
    private static final String STORE_FLOWS    = "rolling-flows-store";

    // ---- PARÂMETROS DE DETECÇÃO ----
    private static final int WINDOW_SIZE = 30;       // "média dos últimos 30" agregados
    private static final double RATIO_REGULAR = 1.2; // 20% acima da média  -> severidade "regular"
    private static final double RATIO_MEDIA   = 1.5; // 50% acima da média  -> severidade "media"
    private static final double RATIO_ALTA    = 2.0; // 100% acima da média -> severidade "alta"

    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "watcher-java-group");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        RollingStatsSerializer   rsSer = new RollingStatsSerializer();
        RollingStatsDeserializer rsDes = new RollingStatsDeserializer();
        WarningSerializer   wSer = new WarningSerializer();
        WarningDeserializer wDes = new WarningDeserializer();

        StreamsBuilder builder = new StreamsBuilder();

        // registra os 3 state stores usados pelo AnomalyDetectorTransformer
        addRollingStore(builder, STORE_INBOUND, rsSer, rsDes);
        addRollingStore(builder, STORE_OUTBOUND, rsSer, rsDes);
        addRollingStore(builder, STORE_FLOWS, rsSer, rsDes);

        // 1) lê o tópico como String (mesmo padrão do App.java/Splitter.java) e faz
        //    o parse manualmente, descartando com log mensagens malformadas em vez
        //    de deixar uma exceção derrubar a StreamThread.
        KStream<String, String> source = builder.stream(INPUT_TOPIC,
                Consumed.with(Serdes.String(), Serdes.String()));

        KStream<String, AggregatedFlow> parsed = source
                .peek((key, value) -> System.out.println("[PEEK-1] Mensagem recebida em " + INPUT_TOPIC))
                .mapValues(value -> {
                    try {
                        return AggregatedFlowParser.fromJsonString(value);
                    } catch (Exception e) {
                        System.err.println("[PARSE] Erro ao fazer parse do AggregatedFlow: " + e.getMessage());
                        return null;
                    }
                })
                .filter((key, value) -> value != null)
                .peek((key, value) -> System.out.println("[PEEK-2] Parse OK! Bytes out: " + value.getBytes_outbound()
                        + " | Bytes in: " + value.getBytes_inbound()
                        + " | Fluxos únicos: " + value.getUnique_flow_count()));

        // 2) para cada AggregatedFlow, compara com a média móvel e gera 0..N warnings
        KStream<String, List<Warning>> warningsLists = parsed.transformValues(
                () -> new AnomalyDetectorTransformer(
                        STORE_INBOUND, STORE_OUTBOUND, STORE_FLOWS,
                        WINDOW_SIZE, RATIO_REGULAR, RATIO_MEDIA, RATIO_ALTA),
                STORE_INBOUND, STORE_OUTBOUND, STORE_FLOWS);

        // 3) "achata" a lista de warnings em registros individuais e publica no tópico de saída
        warningsLists
                .peek((key, list) -> {
                    if (!list.isEmpty())
                        System.out.println("[PEEK-3] " + list.size() + " warning(s) gerado(s) para a chave " + key);
                })
                .flatMapValues(list -> list)
                .peek((key, warning) -> System.out.println("[WARNING] " + warning))
                .to(OUTPUT_TOPIC, Produced.with(Serdes.String(), Serdes.serdeFrom(wSer, wDes)));

        final Topology topology = builder.build();
        System.out.println("=== DESENHO DA TOPOLOGIA ===");
        System.out.println(topology.describe());

        final KafkaStreams streams = new KafkaStreams(topology, props);

        // Igual ao App.java principal: sem isso, qualquer exceção não tratada dentro
        // da topologia (ex: um bug no state store) mata a StreamThread e, se não
        // houver um binding de log configurado no classpath, isso acontece em
        // silêncio total — por isso o handler explícito abaixo.
        streams.setUncaughtExceptionHandler(exception -> {
            System.err.println("[UNCAUGHT EXCEPTION] A StreamThread do OutputWatcher falhou:");
            exception.printStackTrace();
            return StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.REPLACE_THREAD;
        });

        streams.setStateListener((newState, oldState) -> {
            System.out.println("[STATE] " + oldState + " -> " + newState);
        });

        final CountDownLatch latch = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });

        try {
            streams.start();
            latch.await();
        } catch (Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }

    private static void addRollingStore(StreamsBuilder builder, String name,
                                         RollingStatsSerializer ser, RollingStatsDeserializer des) {
        StoreBuilder<KeyValueStore<String, RollingStats>> storeBuilder = Stores.keyValueStoreBuilder(
                Stores.inMemoryKeyValueStore(name),
                Serdes.String(),
                Serdes.serdeFrom(ser, des));
        builder.addStateStore(storeBuilder);
    }
}