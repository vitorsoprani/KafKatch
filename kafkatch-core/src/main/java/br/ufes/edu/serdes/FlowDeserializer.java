package br.ufes.edu.serdes;

import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import br.ufes.edu.domain.Flow;

// CLASSE DEPRECIADA — UTILIZE FlowParser.java
public class FlowDeserializer implements Deserializer<Flow> {
    private final ObjectMapper objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {}

    @Override
    public Flow deserialize(String topic, byte[] data) {
        try {
            if (data == null)
                return null;

            return objectMapper.readValue(data, Flow.class);
        } catch (Exception e) {
            throw new RuntimeException("Erro ao desserializar Flow", e);
        }
    }

    @Override
    public void close() {}
}
