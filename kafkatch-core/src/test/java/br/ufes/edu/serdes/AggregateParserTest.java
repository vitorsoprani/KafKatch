package br.ufes.edu.serdes;

import java.time.LocalDateTime;

import br.ufes.edu.domain.AggregatedFlow;

public class AggregateParserTest {
    public static void main(String[] args) throws Exception {
        AggregatedFlow ida = new AggregatedFlow(LocalDateTime.now());
        System.out.println("Entrada: " + ida.getTimestamp_start());

        String json = AggregatedFlowParser.toJsonString(ida);
        System.out.println(json);

        AggregatedFlow retorno = AggregatedFlowParser.fromJsonString(json);
        System.out.println("Saída: " + retorno.getTimestamp_start());
    }
}
