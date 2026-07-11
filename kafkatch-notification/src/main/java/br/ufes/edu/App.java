package br.ufes.edu;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import com.sshtools.twoslices.Toast;
import com.sshtools.twoslices.ToastType;

import br.ufes.edu.domain.AggregatedFlow;
import br.ufes.edu.serdes.AggregatedFlowParser;

// CLASSE DEPRECIADA — USE OutputWatcher.java
// Para as notificações, use a aplicação em kafkatch-alert-viewers
public class App {
    public static void main(String[] args){
        // CONFIGURAÇÕES DO CONSUMIDOR:
        Properties consProps = new Properties();
        consProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        consProps.put(ConsumerConfig.GROUP_ID_CONFIG, "notifier-java-group");
        consProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");


        // Definições de limite para alerta
        int  sample_max_size = 60;
        long bytes_inbound_limit = 10000000,
             bytes_outbound_limit = 1000000;
        int  unique_flow_limit = 200;
        Notifier notif = new Notifier(sample_max_size, bytes_inbound_limit, bytes_outbound_limit, unique_flow_limit);

        Toast.toast(ToastType.INFO, "Notifier ativado",
            "O Notifier entrou em execução com sucesso");

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consProps)) {
            consumer.subscribe(Collections.singletonList("network-microflows-aggregated"));

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(5));

                for (ConsumerRecord<String,String> record : records) {
                    try {
                        System.out.println("Evento recebido, atualizando estatísticas...");
                        AggregatedFlow a = AggregatedFlowParser.fromJsonString(record.value());

                        notif.addAggregate_sample(a);
                        notif.checkForAnomalies();
                    } catch (Exception e) {
                        System.err.println("Erro ao processar mensagem: " + e.getMessage());
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
