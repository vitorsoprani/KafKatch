package br.ufes.edu;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.serialization.StringDeserializer;

// CLASSE DEPRECIADA — UTILIZE FlowParserTest.java
public class FlowDeserializerTest {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "flow-group");

        // Key é String, Value é Flow
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, FlowDeserializer.class.getName());

        // Read from beginning if no committed offset
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<String, Flow> consumer = new KafkaConsumer<>(props);
        String topic = "topic1";
        consumer.subscribe(Collections.singletonList(topic));

        try {
            while (true) {
                ConsumerRecords<String, Flow> records = consumer.poll(Duration.ofMillis(100));

                for (ConsumerRecord<String, Flow> record : records) {
                    Flow flow = record.value();

                    System.out.println("Flow recebido: " + flow +
                                       " | partition=" + record.partition() +
                                       " | offset=" + record.offset());
                    
                    System.out.println("Flow_key: " + flow.getFlow_key());
                    System.out.println("Src_ip: " + flow.getSrc_ip());
                    System.out.println("Dst_ip: " + flow.getDst_ip());
                    System.out.println("Src_port: " + flow.getSrc_port());
                    System.out.println("Dst_port: " + flow.getDst_port());
                    System.out.println("Protocol: " + flow.getProtocol());

                    System.out.println("Timestamp: " + flow.getTimestamp());

                    System.out.println("Packet_count: " + flow.getPacket_count());
                    System.out.println("Byte_count: " + flow.getByte_count());
                    System.out.println("Min_packet_size: " + flow.getMin_packet_size());
                    System.out.println("Max_packet_size: " + flow.getMax_packet_size());
                }
            }
        } finally {
            consumer.close();
        }
    }
}
