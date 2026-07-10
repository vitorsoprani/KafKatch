package br.ufes.edu.domain;

import java.time.LocalDateTime;
import java.util.HashSet;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonView;

// A maioria dos campos abaixo só tem getter (sem setter). Isso é o suficiente
// para SERIALIZAR (produzir o JSON de saída), mas não para DESSERIALIZAR de
// volta em um objeto Java — o que o Kafka Streams precisa fazer toda vez que
// lê o valor atual do state store durante o aggregate() (já que o cache está
// desligado). Sem isso, a desserialização falha silenciosamente e o agregado
// reinicia do zero a cada evento. Esta anotação permite que o Jackson acesse
// os campos privados diretamente via reflexão quando não há setter.
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
public class AggregatedFlow {
    // Marcador usado com @JsonView para distinguir a serialização "completa"
    // (usada internamente pelo Kafka Streams no state store) da serialização
    // "pública" (usada no JSON final publicado no tópico de saída). Ver
    // AggregatedFlowParser.toInternalJsonString / fromInternalJsonString.
    public static class InternalView {}

    // Janela de tempo
    @JsonFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss.SSS")
    private LocalDateTime timestamp_start;

    @JsonFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss.SSS")
    private LocalDateTime timestamp_end;

    // Estatísticas de Volume
    private long bytes_inbound = 0;
    private long bytes_outbound = 0;
    private long packets_inbound = 0;
    private long packets_outbound = 0;

    // Estatísticas de Conexões
    private int unique_flow_count = 0;
    
    // controle interno de unicidade. Fica de fora do JSON "público" (o que vai
    // pro tópico de saída), mas PRECISA ser incluído na serialização interna
    // usada pelo state store — ver AggregatedFlow.InternalView.
    @JsonView(InternalView.class)
    private Set<String> flowKeys = new HashSet<>();

    // Estatísticas TCP
    private long tcp_syn_total = 0;
    private long tcp_ack_total = 0;
    private long tcp_fin_total = 0;
    private long tcp_rst_total = 0;

    // Construtor
    public AggregatedFlow(LocalDateTime start) {
        this.timestamp_start = start;
    }

    public void addFlow(Flow flow) {
        // 1. Atualiza volume baseado na direção
        if (flow.getDir() == FlowDirection.INBOUND) {
            this.bytes_inbound += flow.getByte_count();
            this.packets_inbound += flow.getPacket_count();
        } else if (flow.getDir() == FlowDirection.OUTBOUND) {
            this.bytes_outbound += flow.getByte_count();
            this.packets_outbound += flow.getPacket_count();
        }

        // 2. Acumula Flags TCP
        this.tcp_syn_total += flow.getTcp_syn_count();
        this.tcp_ack_total += flow.getTcp_ack_count();
        this.tcp_fin_total += flow.getTcp_fin_count();
        this.tcp_rst_total += flow.getTcp_rst_count();

        // 3. Controle de Fluxos Únicos
        // Criamos uma chave única para esse fluxo (IPs + Portas + Protocolo)
        String key = flow.getSrc_ip() + flow.getSrc_port() + 
                     flow.getDst_ip() + flow.getDst_port() + 
                     flow.getProtocol();
        
        if (flowKeys.add(key)) { // .add() retorna true se o item for novo no Set
            this.unique_flow_count++;
        }
        
        this.timestamp_end = LocalDateTime.now();
    }

    public double getAveragePacketSize() {
        long totalPackets = packets_inbound + packets_outbound;
        if (totalPackets == 0) return 0;
        return (double) (bytes_inbound + bytes_outbound) / totalPackets;
    }

    public LocalDateTime getTimestamp_start() {
        return timestamp_start;
    }
    public LocalDateTime getTimestamp_end() {
        return timestamp_end;
    }
    public long getBytes_inbound() {
        return bytes_inbound;
    }
    public long getBytes_outbound() {
        return bytes_outbound;
    }
    public long getPackets_inbound() {
        return packets_inbound;
    }
    public long getPackets_outbound() {
        return packets_outbound;
    }
    public int getUnique_flow_count() {
        return unique_flow_count;
    }
    public long getTcp_syn_total() {
        return tcp_syn_total;
    }
    public long getTcp_ack_total() {
        return tcp_ack_total;
    }
    public long getTcp_fin_total() {
        return tcp_fin_total;
    }
    public long getTcp_rst_total() {
        return tcp_rst_total;
    }

    public void setTimestamp_end(LocalDateTime timestamp) {
        this.timestamp_end = timestamp;
    }
    
    // necessário para o jackson (parser)
    public AggregatedFlow() {}
}