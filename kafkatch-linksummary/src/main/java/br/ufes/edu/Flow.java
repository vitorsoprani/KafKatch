package br.ufes.edu;

import java.io.Serializable;
import java.time.LocalDateTime;

public class Flow implements Serializable {
    // Intervalo englobado no relatorio
    private LocalDateTime timestamp_start,
                          timestamp_end;

    // Informacoes da conexao monitorada
    private String flow_key,
                   src_ip, dst_ip,
                   src_port, dst_port,
                   protocol;

    // Relatorio sobre os pacotes capturados
    private int packet_count, byte_count,
                min_packet_size, max_packet_size;


    
    public LocalDateTime getTimestamp_start() {
        return timestamp_start;
    }
    public void setTimestamp_start(LocalDateTime timestamp_start) {
        this.timestamp_start = timestamp_start;
    }

    public LocalDateTime getTimestamp_end() {
        return timestamp_end;
    }
    public void setTimestamp_end(LocalDateTime timestamp_end) {
        this.timestamp_end = timestamp_end;
    }


    public String getFlow_key() {
        return flow_key;
    }
    public void setFlow_key(String flow_key) {
        this.flow_key = flow_key;
    }

    public String getSrc_ip() {
        return src_ip;
    }
    public void setSrc_ip(String src_ip) {
        this.src_ip = src_ip;
    }

    public String getDst_ip() {
        return dst_ip;
    }
    public void setDst_ip(String dst_ip) {
        this.dst_ip = dst_ip;
    }

    public String getSrc_port() {
        return src_port;
    }
    public void setSrc_port(String src_port) {
        this.src_port = src_port;
    }

    public String getDst_port() {
        return dst_port;
    }
    public void setDst_port(String dst_port) {
        this.dst_port = dst_port;
    }

    public String getProtocol() {
        return protocol;
    }
    public void setProtocol(String protocol) {
        this.protocol = protocol;
    }


    public int getPacket_count() {
        return packet_count;
    }
    public void setPacket_count(int packet_count) {
        this.packet_count = packet_count;
    }

    public int getByte_count() {
        return byte_count;
    }
    public void setByte_count(int byte_count) {
        this.byte_count = byte_count;
    }

    public int getMin_packet_size() {
        return min_packet_size;
    }
    public void setMin_packet_size(int min_packet_size) {
        this.min_packet_size = min_packet_size;
    }

    public int getMax_packet_size() {
        return max_packet_size;
    }
    public void setMax_packet_size(int max_packet_size) {
        this.max_packet_size = max_packet_size;
    }


    // Para uso pelo desserializador
    public Flow() {}

    public Flow(LocalDateTime _timestamp_start, LocalDateTime _timestamp_end,
                String _flow_key, String _src_ip, String _dst_ip,
                String _src_port, String _dst_port, String _protocol,
                int _packet_count, int _byte_count,
                int _min_packet_size, int _max_packet_size) {
        setTimestamp_start(_timestamp_start);
        setTimestamp_end(_timestamp_end);
        
        setFlow_key(_flow_key);
        setSrc_ip(_src_ip);
        setDst_ip(_dst_ip);
        setSrc_port(_src_port);
        setDst_port(_dst_port);
        setProtocol(_protocol);

        setPacket_count(_packet_count);
        setByte_count(_byte_count);
        setMin_packet_size(_min_packet_size);
        setMax_packet_size(_max_packet_size);
    }
}
