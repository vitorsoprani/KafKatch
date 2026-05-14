package br.ufes.edu;

import java.time.LocalDateTime;

public class AggregateParserTest {
    public static void main(String[] args) throws Exception {
        Aggregate ida = new Aggregate(LocalDateTime.now());
        System.out.println("Entrada: " + ida.getTimestamp_start());

        String json = AggregateParser.toJsonString(ida);
        System.out.println(json);

        Aggregate retorno = AggregateParser.fromJsonString(json);
        System.out.println("Saída: " + retorno.getTimestamp_start());
    }
}
