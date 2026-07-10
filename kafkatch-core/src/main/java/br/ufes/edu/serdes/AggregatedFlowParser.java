package br.ufes.edu.serdes;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import br.ufes.edu.domain.AggregatedFlow;
import br.ufes.edu.domain.AggregatedFlow.InternalView;

public class AggregatedFlowParser {
    private static final ObjectMapper objectMapper = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    // JSON "público": usado para o payload final publicado no tópico de saída.
    // Sem nenhuma view ativa, campos anotados com @JsonView (como o flowKeys
    // de AggregatedFlow) ficam automaticamente de fora — mesmo comportamento
    // que o @JsonIgnore tinha antes.
    public static String toJsonString(AggregatedFlow aggregatedFlow) throws Exception {
        return objectMapper.writeValueAsString(aggregatedFlow);
    }

    public static AggregatedFlow fromJsonString(String json) throws Exception {
        return objectMapper.readValue(json, AggregatedFlow.class);
    }

    // JSON "interno": usado SOMENTE pelo serde do state store do Kafka Streams
    // (Materialized.with(...)). Com a InternalView ativa, o flowKeys também é
    // incluído, o que é necessário para que a contagem de fluxos únicos
    // sobreviva ao round-trip de serialização/desserialização que acontece a
    // cada evento (já que o cache do state store está desligado).
    public static String toInternalJsonString(AggregatedFlow aggregatedFlow) throws Exception {
        return objectMapper.writerWithView(InternalView.class).writeValueAsString(aggregatedFlow);
    }

    public static AggregatedFlow fromInternalJsonString(String json) throws Exception {
        return objectMapper.readerWithView(InternalView.class)
                .forType(AggregatedFlow.class)
                .readValue(json);
    }
}