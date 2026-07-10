package br.ufes.edu;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

public class AggregatedFlowParser {
// Adicionamos o .disable(...) aqui!
    private static final ObjectMapper objectMapper = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

    public static String toJsonString(AggregatedFlow aggregatedFlow) throws Exception {
        return objectMapper.writeValueAsString(aggregatedFlow);
    }

    public static AggregatedFlow fromJsonString(String json) throws Exception {
        return objectMapper.readValue(json, AggregatedFlow.class);
    }
}
