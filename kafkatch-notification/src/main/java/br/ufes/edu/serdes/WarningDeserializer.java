package br.ufes.edu.serdes;

import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;

import com.fasterxml.jackson.databind.ObjectMapper;

import br.ufes.edu.domain.Warning;

public class WarningDeserializer implements Deserializer<Warning> {
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {}

    @Override
    public Warning deserialize(String topic, byte[] data) {
        try {
            if (data == null)
                return null;

            return objectMapper.readValue(data, Warning.class);
        } catch (Exception e) {
            throw new RuntimeException("Erro ao desserializar Warning", e);
        }
    }
}