package br.ufes.edu;

import java.time.LocalDateTime;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class FlowSerializerTest {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        // Key é String, Value é Flow
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, FlowSerializer.class.getName());

        KafkaProducer<String, Flow> producer = new KafkaProducer<>(props);
        String topic = "topic1";

        try {
            Flow flow = new Flow(LocalDateTime.MIN, LocalDateTime.MAX,
                                 "key", "src_ip", "dst_ip", "src_port",
                                 "dst_port", "protocol", 2, 20, 8, 12);

            ProducerRecord<String, Flow> record = new ProducerRecord<String,Flow>(topic, flow);

            producer.send(record, (metadata, exception) -> {
                if (exception == null) {
                    System.out.println("Enviado: " + flow +
                        " | partition=" + metadata.partition() +
                        " offset=" + metadata.offset());
                } else {
                    exception.printStackTrace();
                }
            });
        } finally {
            producer.close();
        }
    }
}
