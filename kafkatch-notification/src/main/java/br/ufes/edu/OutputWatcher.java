package br.ufes.edu;

import java.time.Duration;
import java.util.LongSummaryStatistics;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.SlidingWindows;
import org.apache.kafka.streams.state.WindowStore;

import br.ufes.edu.domain.LatestStat;
import br.ufes.edu.serdes.LongSummaryStatisticsDeserializer;
import br.ufes.edu.serdes.LongSummaryStatisticsSerializer;

public class OutputWatcher {
    public static void main(String[] args) throws Exception {
        // SERDES PERSONALIZADOS
        LongSummaryStatisticsSerializer   longSumStatSer = new LongSummaryStatisticsSerializer();
        LongSummaryStatisticsDeserializer longSumStatDes = new LongSummaryStatisticsDeserializer();

        // CRITÉRIOS DE PRODUÇÃO
        long   bytes_outbound_limit = 100000L,
               sample_size_threshold = 3;
        double entry_to_average_ratio_limit = 1.5;

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

        KTable<String, LongSummaryStatistics> stats = source.peek((key, value) -> System.out.println(key + " : " + value)).groupByKey().windowedBy(SlidingWindows.ofTimeDifferenceAndGrace(Duration.ofSeconds(10), Duration.ofSeconds(5)))
                                                            .aggregate(() -> new LongSummaryStatistics(),
                                                                        (aggKey, newValue, aggValue) -> { aggValue.accept(newValue);
                                                                                                          return aggValue; },
                                                                        Materialized.<String, LongSummaryStatistics, WindowStore<Bytes, byte[]>>as("windowed-outbound-statistics-store")
                                                            .withKeySerde(Serdes.String()).withValueSerde(Serdes.serdeFrom(longSumStatSer, longSumStatDes)))
                                                            .toStream().peek((key, value) -> System.out.println(key + " : " + value.getCount())).map((windowedKey, value) -> new KeyValue<>(windowedKey.key(), value)).groupByKey()
                                                            .reduce((aggValue, newValue) -> newValue, Materialized.with(Serdes.String(), Serdes.serdeFrom(longSumStatSer, longSumStatDes)));
        source.join(stats, (leftValue, rightValue) -> new LatestStat(leftValue, rightValue))
              .filter((key, value) -> (value.getSample_size() > sample_size_threshold) &&
                                      (value.getEntry_to_average_ratio() > entry_to_average_ratio_limit))
              .mapValues((value) -> "O último agregado registrou " + value.getLatest_entry() + " bytes enviados, " +
                                    (int)(value.getEntry_to_average_ratio()*100 - 100) + "% acima da média de " +
                                    Math.round(value.getStats_average()) + " bytes enviados nas " + value.getSample_size() + " agregações anteriores")
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
