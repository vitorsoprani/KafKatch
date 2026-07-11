package br.ufes.edu;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Branched;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;

import br.ufes.edu.domain.AggregatedFlow;
import br.ufes.edu.serdes.AggregatedFlowParser;

// CLASSE DEPRECIADA — OutputWatcher.java REALIZA O PROCESSAMENTO INTEIRO
public class Splitter {
    public static void main(String[] args) throws Exception {
        // CONFIGURAÇÕES DO CONSUMIDOR
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "splitter-java-group");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");


        final StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> source = builder.stream("network-microflows-aggregated");
        KStream<String, AggregatedFlow> parsed = source.peek((key, value) -> System.out.println(key + " : " + value))
                                                       .mapValues(value -> { AggregatedFlow a = null;
                                                                             try {
                                                                                 a = AggregatedFlowParser.fromJsonString(value);
                                                                             }
                                                                             catch (Exception e) {
                                                                                 System.out.println("Erro ao ler JSON de AggregatedFlow");
                                                                             }
                                                                             return a; });
        Map<String, KStream<String, Long>> branches = parsed.flatMap((key, value) -> { List<KeyValue<String, Long>> result = new LinkedList<>();
                                                                                        result.add(KeyValue.pair("in", value.getBytes_inbound()));
                                                                                        result.add(KeyValue.pair("out", value.getBytes_outbound()));
                                                                                        return result; })
                                                            .peek((key, value) -> System.out.println(key + " : " + value))
                                                            .split(Named.as("branch-")).branch((key, value) -> key.startsWith("in"), Branched.as("in"))
                                                                                .branch((key, value) -> key.startsWith("out"), Branched.as("out"))
                                                                                .noDefaultBranch();
        KStream<String, Integer> flowRecords = parsed.map((key, value) -> KeyValue.pair("flow", value.getUnique_flow_count()))
                                                     .peek((key, value) -> System.out.println(key + " : " + value));
                                                                                            
        branches.get("branch-in").to("network-summary-bytes-inbound", Produced.with(Serdes.String(), Serdes.Long()));
        branches.get("branch-out").to("network-summary-bytes-outbound", Produced.with(Serdes.String(), Serdes.Long()));
        flowRecords.to("network-summary-unique-flow-count", Produced.with(Serdes.String(), Serdes.Integer()));

        final Topology topology = builder.build();
        System.out.println(topology.describe());

        final KafkaStreams streams = new KafkaStreams(topology, props);


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
}
