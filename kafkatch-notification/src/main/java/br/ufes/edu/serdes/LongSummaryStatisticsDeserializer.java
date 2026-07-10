package br.ufes.edu.serdes;

import java.util.LongSummaryStatistics;
import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;

import com.fasterxml.jackson.databind.ObjectMapper;

public class LongSummaryStatisticsDeserializer implements Deserializer<LongSummaryStatistics> {
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {}

    @Override
    public LongSummaryStatistics deserialize(String topic, byte[] data) {
        try {
            if (data == null)
                return null;

            return objectMapper.readValue(data, LongSummaryStatistics.class);
        } catch (Exception e) {
            throw new RuntimeException("Erro ao desserializar LongSummaryStatistics", e);
        }
    }
}
