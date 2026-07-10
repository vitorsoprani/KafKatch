package br.ufes.edu;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

public class OutputWatcher {
    public static void main(String[] args) throws Exception {
        // CRITÉRIOS DE PRODUÇÃO
        long bytes_outbound_limit = 10000000L;

        // CONFIGURAÇÕES DO CONSUMIDOR
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "watcher-java-group");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Long().getClass());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");


        final StreamsBuilder builder = new StreamsBuilder();

        KStream<String, Long> source = builder.stream("network-summary-bytes-outbound");

        source.filter((key, value) -> value > bytes_outbound_limit)
              .mapValues(value -> "O último agregado registrou " + value + " bytes enviados")
              .peek((key, value) -> System.out.println(key + " : " + value))
              .to("network-warnings", Produced.with(Serdes.String(), Serdes.String()));

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
