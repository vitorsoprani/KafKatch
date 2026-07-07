package br.ufes.edu.serdes;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import br.ufes.edu.domain.AggregatedFlow;

public class AggregatedFlowParser {
    private static final ObjectMapper objectMapper = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    public static String toJsonString(AggregatedFlow aggregatedFlow) throws Exception {
        return objectMapper.writeValueAsString(aggregatedFlow);
    }

    public static AggregatedFlow fromJsonString(String json) throws Exception {
        return objectMapper.readValue(json, AggregatedFlow.class);
    }
}
