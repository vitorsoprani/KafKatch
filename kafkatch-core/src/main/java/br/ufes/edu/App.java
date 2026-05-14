package br.ufes.edu;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class App {
    public static void main(String[] args) {
        
        Properties props = new Properties();
        
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "agregador-java-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList("network-microflows-raw"));
            System.out.println("Consumidor iniciado. Aguardando pacotes do tópico 'network-microflows-raw'...");

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<String, String> record : records) {
                    String jsonCru = record.value();
                    
                    long timestampKafka = record.timestamp();

                    try {
                        Flow microFlow = FlowParser.readJson(jsonCru, timestampKafka);
                        
                        System.out.println("=====================================");
                        System.out.println("Hora da Captura : " + microFlow.getTimestamp());
                        System.out.println("Origem          : " + microFlow.getSrc_ip() + ":" + microFlow.getSrc_port());
                        System.out.println("Destino         : " + microFlow.getDst_ip() + ":" + microFlow.getDst_port());
                        System.out.println("Protocolo       : " + microFlow.getProtocol());
                        System.out.println("Direção         : " + microFlow.getDir());
                        System.out.println("Tamanho (Bytes) : " + microFlow.getByte_count());
                        System.out.println("=====================================\n");
                        
                    } catch (Exception e) {
                        System.err.println("Erro ao processar o JSON: " + jsonCru);
                        e.printStackTrace();
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}