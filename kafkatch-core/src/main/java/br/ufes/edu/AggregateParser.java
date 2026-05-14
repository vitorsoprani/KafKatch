package br.ufes.edu;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

public class AggregateParser {
    private static final ObjectMapper objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());

    public static String toJsonString(Aggregate aggregate) throws Exception {
        return objectMapper.writeValueAsString(aggregate);
    }

    public static Aggregate fromJsonString(String json) throws Exception {
        return objectMapper.readValue(json, Aggregate.class);
    }
}
