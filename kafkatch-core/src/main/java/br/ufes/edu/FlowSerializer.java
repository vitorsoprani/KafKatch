package br.ufes.edu;

import java.util.Map;

import org.apache.kafka.common.serialization.Serializer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

// CLASSE DEPRECIADA — UTILIZE FlowParser.java
public class FlowSerializer implements Serializer<Flow> {
    private final ObjectMapper objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {}

    @Override
    public byte[] serialize(String topic, Flow data) {
        try {
            if (data == null)
                return null;

            return objectMapper.writeValueAsBytes(data);
        } catch (Exception e) {
            throw new RuntimeException("Erro ao serializar Flow", e);
        }
    }

    @Override
    public void close() {}
}
