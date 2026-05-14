package br.ufes.edu;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class FlowParser {
    
    // Instância única do ObjectMapper (pesada de criar, boa prática deixar estática)
    private static final ObjectMapper mapper = new ObjectMapper();

    public static Flow readJson(String json, long kafka_timestamp) throws Exception {
        
        // Lê a string inteira e transforma numa árvore de "Nós" do Jackson
        JsonNode root = mapper.readTree(json);

        String src_ip = root.get("src_ip").asText();
        String dst_ip = root.get("dst_ip").asText();
        short src_port = (short) root.get("src_port").asInt();
        short dst_port = (short) root.get("dst_port").asInt();
        
        FlowProtocol protocol = FlowProtocol.valueOf(root.get("protocol").asText());
        FlowDirection dir = FlowDirection.valueOf(root.get("dir").asText());

        int packets = root.get("packets").asInt();
        int bytes = root.get("bytes").asInt();
        short min_size = (short) root.get("min_size").asInt();
        short max_size = (short) root.get("max_size").asInt();
        
        int nanoOfSecond = (int)(kafka_timestamp % 1000) * 1000000;
        LocalDateTime timestamp = LocalDateTime.ofInstant(
            Instant.ofEpochMilli(nanoOfSecond),
            ZoneOffset.UTC
        );
        
        double mean_iat_us = root.get("mean_iat_us").asDouble();

        // Navegando para dentro do objeto "tcp"
        JsonNode tcp = root.get("tcp");
        int tcp_syn_count = tcp.get("syn").asInt();
        int tcp_fin_count = tcp.get("fin").asInt();
        int tcp_rst_count = tcp.get("rst").asInt();
        int tcp_ack_count = tcp.get("ack").asInt();

        // Constrói e retorna o objeto Flow com todos os dados
        return new Flow(src_ip, dst_ip,
                        src_port, dst_port,
                        protocol, dir,
                        packets, bytes,
                        min_size, max_size,
                        timestamp, mean_iat_us,
                        tcp_syn_count, tcp_fin_count,
                        tcp_rst_count, tcp_ack_count);
    }
}