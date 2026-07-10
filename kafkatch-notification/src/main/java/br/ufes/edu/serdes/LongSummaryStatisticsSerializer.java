package br.ufes.edu.serdes;

import java.util.LongSummaryStatistics;
import java.util.Map;

import org.apache.kafka.common.serialization.Serializer;

import com.fasterxml.jackson.databind.ObjectMapper;

public class LongSummaryStatisticsSerializer implements Serializer<LongSummaryStatistics> {
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {}

    @Override
    public byte[] serialize(String topic, LongSummaryStatistics data) {
        try {
            if (data == null)
                return null;

            return objectMapper.writeValueAsBytes(data);
        } catch (Exception e) {
            throw new RuntimeException("Erro ao serializar LongSummaryStatistics", e);
        }
    }

    @Override
    public void close() {}
}
