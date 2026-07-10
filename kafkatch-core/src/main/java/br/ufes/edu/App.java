package br.ufes.edu;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Suppressed;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Suppressed.BufferConfig;

import com.fasterxml.jackson.databind.ObjectMapper;

import br.ufes.edu.domain.AggregatedFlow;
import br.ufes.edu.domain.Flow;
import br.ufes.edu.serdes.AggregatedFlowParser;
import br.ufes.edu.serdes.FlowParser;

// import java.time.Duration;
// import java.time.LocalDateTime;
// import java.util.Collections;
// import java.util.Properties;

// import org.apache.kafka.clients.consumer.ConsumerConfig;
// import org.apache.kafka.clients.consumer.ConsumerRecord;
// import org.apache.kafka.clients.consumer.ConsumerRecords;
// import org.apache.kafka.clients.consumer.KafkaConsumer;
// import org.apache.kafka.clients.producer.KafkaProducer;
// import org.apache.kafka.clients.producer.ProducerConfig;
// import org.apache.kafka.clients.producer.ProducerRecord;
// import org.apache.kafka.common.serialization.StringDeserializer;
// import org.apache.kafka.common.serialization.StringSerializer;

// import br.ufes.edu.domain.AggregatedFlow;
// import br.ufes.edu.domain.Flow;
// import br.ufes.edu.serdes.AggregatedFlowParser;
// import br.ufes.edu.serdes.FlowParser;

public class App {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "agregador-streams-app-v2");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100);
        props.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        
        final String inputTopic = "network-microflows-raw";
        final String outputTopic = "network-microflows-aggregated";

        final Integer windowSize = 10;

        ObjectMapper mapper = new ObjectMapper();

        // SERDE PARA FLOW
        Serializer<Flow> flowSerializer = (topic, data) -> {
            if (data == null) return null;
            try {
                return mapper.writeValueAsBytes(data);
            } catch (Exception e) {
                return null;
            }
        };
        Deserializer<Flow> flowDeserializer = (topic, data) -> {
            if (data == null) return null;
            try {
                // IMPORTANTE: o flowSerializer acima usa o Jackson genérico
                // (mapper.writeValueAsBytes) para serializar o Flow no tópico de
                // repartição criado pelo groupBy(). O FlowParser.readJson foi feito
                // para o formato BRUTO vindo do sniffer, que é diferente do JSON
                // gerado pelo Jackson a partir do objeto Flow. Usar FlowParser aqui
                // fazia o parse falhar silenciosamente (retornando null), o que por
                // sua vez causava NullPointerException dentro do aggregate() e
                // derrubava a StreamThread sem log nenhum. A correção é usar o
                // Jackson simetricamente aqui também.
                return mapper.readValue(data, Flow.class);
            } catch (Exception e) {
                System.err.println("[flowDeserializer] Falha ao desserializar Flow do tópico de reparticionamento: " + e.getMessage());
                return null;
            }
        };
        Serde<Flow> flowSerde = Serdes.serdeFrom(flowSerializer, flowDeserializer);
 
        // SERDE PARA AGGREGATEDFLOW
        Serializer<AggregatedFlow> aggSerializer = (topic, data) -> {
            if (data == null) return null;
            try {
                return AggregatedFlowParser.toJsonString(data).getBytes("UTF-8");
            } catch (Exception e) {
                return null;
            }
        };
        Deserializer<AggregatedFlow> aggDeserializer = (topic, data) -> {
            if (data == null) return null;
            try {
                return mapper.readValue(data, AggregatedFlow.class);
            } catch (Exception e) {
                return null;
            }
        };
        Serde<AggregatedFlow> aggregatedFlowSerde = Serdes.serdeFrom(aggSerializer, aggDeserializer);

        // TOPOLOGIA KAFKA STREAMS:
        StreamsBuilder builder = new StreamsBuilder();
        builder.<String, String>stream(inputTopic)
            .peek((key, value) -> System.out.println("[PEEK-1] Mensagem recebida"))
            .mapValues((readOnlyKey, value) -> {
                try {
                    return FlowParser.readJson(value, System.currentTimeMillis());
                } catch (Exception e) {
                    System.err.println("Erro no parse: " + e.getMessage());
                    return null;
                }
            })
            .filter((key, flow) -> flow != null)
            .peek((key, flow) -> System.out.println("[PEEK-2] Parse OK! Fluxo recebido - Bytes: " + flow.getByte_count()))
            .groupBy((key, flow) -> "global-key", Grouped.with(Serdes.String(), flowSerde))
            .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofSeconds(windowSize)))
            .aggregate(
                () -> new AggregatedFlow(LocalDateTime.now()),
                (key, flow, aggregate) -> {
                    if (flow == null) {
                        System.err.println("[aggregate] Flow nulo recebido, ignorando registro.");
                        return aggregate;
                    }
                    aggregate.addFlow(flow);
                    return aggregate;
                },
                Materialized.with(Serdes.String(), aggregatedFlowSerde)
            )
            .suppress(Suppressed.untilWindowCloses(BufferConfig.unbounded()))
            .toStream()
            .peek((windowedKey, aggregatedFlow) -> System.out.println("[PEEK-3] Janela fechada! Liberando agregado das " + windowedKey.window().startTime()))
            .map((windowedKey, aggregatedFlow) -> {
                try {
                    String jsonOut = AggregatedFlowParser.toJsonString(aggregatedFlow);
                    System.out.println("Agregado enviado! Bytes In: " + aggregatedFlow.getBytes_inbound() + 
                                    " | Flows Únicos: " + aggregatedFlow.getUnique_flow_count());
                    return new KeyValue<>(windowedKey.key(), jsonOut);
                } catch (Exception e) {
                    return new KeyValue<>(windowedKey.key(), null);
                }
            })
            .filter((key, jsonOut) -> jsonOut != null)
            .to(outputTopic,Produced.with(Serdes.String(), Serdes.String()));
       
        final Topology topology = builder.build();

        System.out.println("=== DESENHO DA TOPOLOGIA ===");
        System.out.println(topology.describe());
        
        KafkaStreams streams = new KafkaStreams(topology, props);

        streams.setUncaughtExceptionHandler(exception -> {
            System.err.println("[UNCAUGHT EXCEPTION] A StreamThread falhou:");
            exception.printStackTrace();
            return org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.REPLACE_THREAD;
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
            //inicia a aplicação de streams, com a topologia definida
            streams.start();
            //ela fica executando, até que seja explicitamente interrompida
            latch.await();
        } catch (Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }
}