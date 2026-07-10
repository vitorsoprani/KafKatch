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

import br.ufes.edu.domain.Warning;
import br.ufes.edu.serdes.WarningDeserializer;

/**
 * Consumidor "burro", no mesmo estilo do App.java antigo (KafkaConsumer puro,
 * sem Kafka Streams). Toda a lógica de detecção de anomalia já foi feita rio
 * acima, pelo AnomalyDetectorTransformer dentro do kafkatch-notification
 * (módulo OutputWatcher). Aqui só resta ler os Warnings já prontos do tópico
 * network-warnings e transformar cada um em um popup no desktop.
 */
public class App {
    public static void main(String[] args) {
        // CONFIGURAÇÕES DO CONSUMIDOR:
        Properties consProps = new Properties();
        consProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        consProps.put(ConsumerConfig.GROUP_ID_CONFIG, "alert-viewer-java-group");
        consProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, WarningDeserializer.class.getName());
        consProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        Toast.toast(ToastType.INFO, "Alert Viewer ativado",
            "O Alert Viewer entrou em execução com sucesso");

        try (KafkaConsumer<String, Warning> consumer = new KafkaConsumer<>(consProps)) {
            consumer.subscribe(Collections.singletonList("network-warnings"));
            System.out.println("[INIT] Inscrito em network-warnings, aguardando mensagens...");

            while (true) {
                ConsumerRecords<String, Warning> records;

                try {
                    // IMPORTANTE: o WarningDeserializer lança RuntimeException se o JSON
                    // vier malformado, e isso acontece DENTRO do poll() (a desserialização
                    // ocorre antes do poll() devolver o lote de registros). Por isso esse
                    // try/catch precisa envolver o poll() em si, e não só o for abaixo —
                    // senão uma única mensagem malformada derrubaria o consumidor inteiro.
                    records = consumer.poll(Duration.ofSeconds(5));
                } catch (Exception e) {
                    System.err.println("[POLL] Erro ao consumir/desserializar mensagens: " + e.getMessage());
                    continue;
                }

                for (ConsumerRecord<String, Warning> record : records) {
                    try {
                        Warning warning = record.value();

                        if (warning == null) {
                            System.err.println("[PROCESS] Warning nulo recebido, ignorando registro.");
                            continue;
                        }

                        System.out.println("[PROCESS] Warning recebido: " + warning);
                        Notifier.notify(warning);
                    } catch (Exception e) {
                        System.err.println("[PROCESS] Erro ao processar mensagem: " + e.getMessage());
                    }
                }
            }
        } catch (Exception e) {
            System.err.println("[FATAL] Consumer encerrado por erro:");
            e.printStackTrace();
        }
    }
}