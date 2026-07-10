package br.ufes.edu.serdes;

import java.util.Map;

import org.apache.kafka.common.serialization.Serializer;

import com.fasterxml.jackson.databind.ObjectMapper;

import br.ufes.edu.domain.RollingStats;

public class RollingStatsSerializer implements Serializer<RollingStats> {
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {}

    @Override
    public byte[] serialize(String topic, RollingStats data) {
        try {
            if (data == null)
                return null;

            return objectMapper.writeValueAsBytes(data);
        } catch (Exception e) {
            throw new RuntimeException("Erro ao serializar RollingStats", e);
        }
    }

    @Override
    public void close() {}
}