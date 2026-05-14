package br.ufes.edu;

import java.time.LocalDateTime;

public class Aggregate {
    // Resumo das conexoes monitoradas
    private int flow_key_count,
                src_ip_count, dst_ip_count,
                src_port_count, dst_port_count;

    // Relatório sobre os pacotes capturados
    private final int[] protocol_count = new int[FlowProtocol.values().length],
                        dir_count = new int[FlowDirection.values().length];
    private int   packet_count, byte_count;
    private short min_packet_size, max_packet_size;

    // Informações de tempo
    private LocalDateTime timestamp_start,
                          timestamp_end;

    // Distribuição dos pacotes (para TCP)
    private int tcp_syn_count, tcp_fin_count,
                tcp_rst_count, tcp_ack_count;



    public int getFlow_key_count() {
        return flow_key_count;
    }
    public void setFlow_key_count(int flow_key_count) {
        this.flow_key_count = flow_key_count;
    }
    public void incrementFlow_key_count() {
        flow_key_count++;
    }

    public int getSrc_ip_count() {
        return src_ip_count;
    }
    public void setSrc_ip_count(int src_ip_count) {
        this.src_ip_count = src_ip_count;
    }
    public void incrementSrc_ip_count() {
        src_ip_count++;
    }

    public int getDst_ip_count() {
        return dst_ip_count;
    }
    public void setDst_ip_count(int dst_ip_count) {
        this.dst_ip_count = dst_ip_count;
    }
    public void incrementDst_ip_count() {
        dst_ip_count++;
    }

    public int getSrc_port_count() {
        return src_port_count;
    }
    public void setSrc_port_count(int src_port_count) {
        this.src_port_count = src_port_count;
    }
    public void incrementSrc_port_count() {
        src_port_count++;
    }

    public int getDst_port_count() {
        return dst_port_count;
    }
    public void setDst_port_count(int dst_port_count) {
        this.dst_port_count = dst_port_count;
    }
    public void incrementDst_port_count() {
        dst_port_count++;
    }


    public int[] getProtocol_count() {
        return protocol_count;
    }
    public void setProtocol_count(int amount, int index) {
        protocol_count[index] = amount;
    }
    public void addProtocol_count(int amount, int index) {
        protocol_count[index] += amount;
    }

    public int[] getDir_count() {
        return dir_count;
    }
    public void setDir_count(int amount, int index) {
        dir_count[index] = amount;
    }
    public void addDir_count(int amount, int index) {
        dir_count[index] += amount;
    }

    public int getPacket_count() {
        return packet_count;
    }
    public void setPacket_count(int packet_count) {
        this.packet_count = packet_count;
    }
    public void addPacket_count(int amount) {
        packet_count += amount;
    }

    public int getByte_count() {
        return byte_count;
    }
    public void setByte_count(int byte_count) {
        this.byte_count = byte_count;
    }
    public void addByte_count(int amount) {
        byte_count += amount;
    }

    public short getMin_packet_size() {
        return min_packet_size;
    }
    public void setMin_packet_size(short min_packet_size) {
        this.min_packet_size = min_packet_size;
    }

    public short getMax_packet_size() {
        return max_packet_size;
    }
    public void setMax_packet_size(short max_packet_size) {
        this.max_packet_size = max_packet_size;
    }


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


    public int getTcp_syn_count() {
        return tcp_syn_count;
    }
    public void setTcp_syn_count(int tcp_syn_count) {
        this.tcp_syn_count = tcp_syn_count;
    }
    public void addTcp_syn_count(int amount) {
        tcp_syn_count += amount;
    }

    public int getTcp_fin_count() {
        return tcp_fin_count;
    }
    public void setTcp_fin_count(int tcp_fin_count) {
        this.tcp_fin_count = tcp_fin_count;
    }
    public void addTcp_fin_count(int amount) {
        tcp_fin_count += amount;
    }

    public int getTcp_rst_count() {
        return tcp_rst_count;
    }
    public void setTcp_rst_count(int tcp_rst_count) {
        this.tcp_rst_count = tcp_rst_count;
    }
    public void addTcp_rst_count(int amount) {
        tcp_rst_count += amount;
    }

    public int getTcp_ack_count() {
        return tcp_ack_count;
    }
    public void setTcp_ack_count(int tcp_ack_count) {
        this.tcp_ack_count = tcp_ack_count;
    }
    public void addTcp_ack_count(int amount) {
        tcp_ack_count += amount;
    }


    public Aggregate(LocalDateTime _timestamp_start) {
        setFlow_key_count(0);
        setSrc_ip_count(0);
        setDst_ip_count(0);
        setSrc_port_count(0);
        setDst_port_count(0);
        
        // set protocol dir

        setPacket_count(0);
        setByte_count(0);
        setMax_packet_size((short)0);
        setMin_packet_size(Short.MAX_VALUE);

        setTimestamp_start(_timestamp_start);
        setTimestamp_end(timestamp_start);

        setTcp_syn_count(0);
        setTcp_fin_count(0);
        setTcp_rst_count(0);
        setTcp_ack_count(0);
    }
}
