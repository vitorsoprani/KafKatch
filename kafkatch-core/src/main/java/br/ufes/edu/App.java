package br.ufes.edu;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

public class App {
    public static void main(String[] args) {
        

        // CONFIGURAÇÕES DO CONSUMIDOR:
        Properties consProps = new Properties();
        consProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        consProps.put(ConsumerConfig.GROUP_ID_CONFIG, "agregador-java-group");
        consProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        // CONFIGURAÇÕES DO PRODUTOR
        Properties prodProps = new Properties();
        prodProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        prodProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        prodProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        long windowSizeMs = 30000; // 60 segundos
        long lastWindowFlush = System.currentTimeMillis();
        AggregatedFlow currentAggregate = new AggregatedFlow(LocalDateTime.now());

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consProps);
             KafkaProducer<String, String> producer = new KafkaProducer<>(prodProps)) {

            consumer.subscribe(Collections.singletonList("network-microflows-raw"));
            System.out.println("Agregador iniciado. Janela de 60s ativa...");

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));

                for (ConsumerRecord<String, String> record : records) {
                    try {
                        Flow flow = FlowParser.readJson(record.value(), record.timestamp());

                        System.out.println("=====================================");
                        System.out.println("Hora da Captura : " + flow.getTimestamp());
                        System.out.println("Origem          : " + flow.getSrc_ip() + ":" + flow.getSrc_port());
                        System.out.println("Destino         : " + flow.getDst_ip() + ":" + flow.getDst_port());
                        System.out.println("Protocolo       : " + flow.getProtocol());
                        System.out.println("Direção         : " + flow.getDir());
                        System.out.println("Tamanho (Bytes) : " + flow.getByte_count());
                        System.out.println("=====================================\n");
                        
                        currentAggregate.addFlow(flow);
                    } catch (Exception e) {
                        System.err.println("Erro ao processar mensagem: " + e.getMessage());
                    }
                }

                long currentTime = System.currentTimeMillis();
                if (currentTime - lastWindowFlush >= windowSizeMs) {
                    System.out.println("--- Fechando janela de 60s: " + currentAggregate.getTimestamp_start() + " ---");

                    try {
                        String jsonOut = AggregatedFlowParser.toJsonString(currentAggregate);
                        
                        producer.send(new ProducerRecord<>("network-microflows-aggregated", jsonOut));
                        
                        System.out.println("Agregado enviado! Bytes In: " + currentAggregate.getBytes_inbound() + 
                                           " | Flows Únicos: " + currentAggregate.getUnique_flow_count());

                    } catch (Exception e) {
                        System.err.println("Erro ao enviar agregado: " + e.getMessage());
                    }

                    // 5. Reinicia a janela
                    lastWindowFlush = currentTime;
                    currentAggregate = new AggregatedFlow(LocalDateTime.now());
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}