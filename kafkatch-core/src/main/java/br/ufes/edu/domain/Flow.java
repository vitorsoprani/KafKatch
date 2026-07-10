package br.ufes.edu.domain;

import java.io.Serializable;
import java.time.LocalDateTime;

public class Flow implements Serializable {
    // Informacoes da conexao monitorada
    private String flow_key,
                   src_ip, dst_ip;
    private int  src_port, dst_port;
    private FlowProtocol  protocol;
    private FlowDirection dir;

    // Relatório sobre os pacotes capturados
    private int   packet_count, byte_count;
    private int min_packet_size, max_packet_size;

    // Informações de tempo
    private LocalDateTime timestamp;
    private double mean_iat_us;

    // Distribuição dos pacotes (para TCP)
    private int tcp_syn_count, tcp_fin_count,
                tcp_rst_count, tcp_ack_count;


    
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

    public int getSrc_port() {
        return src_port;
    }
    public void setSrc_port(int src_port) {
        this.src_port = src_port;
    }

    public int getDst_port() {
        return dst_port;
    }
    public void setDst_port(int dst_port) {
        this.dst_port = dst_port;
    }

    public FlowProtocol getProtocol() {
        return protocol;
    }
    public void setProtocol(FlowProtocol protocol) {
        this.protocol = protocol;
    }

    public FlowDirection getDir() {
        return dir;
    }
    public void setDir(FlowDirection dir) {
        this.dir = dir;
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

    
    public LocalDateTime getTimestamp() {
        return timestamp;
    }
    public void setTimestamp(LocalDateTime timestamp) {
        this.timestamp = timestamp;
    }

    public double getMean_iat_us() {
        return mean_iat_us;
    }
    public void setMean_iat_us(double mean_iat_us) {
        this.mean_iat_us = mean_iat_us;
    }


    public int getTcp_syn_count() {
        return tcp_syn_count;
    }
    public void setTcp_syn_count(int tcp_syn_count) {
        this.tcp_syn_count = tcp_syn_count;
    }

    public int getTcp_fin_count() {
        return tcp_fin_count;
    }
    public void setTcp_fin_count(int tcp_fin_count) {
        this.tcp_fin_count = tcp_fin_count;
    }

    public int getTcp_rst_count() {
        return tcp_rst_count;
    }
    public void setTcp_rst_count(int tcp_rst_count) {
        this.tcp_rst_count = tcp_rst_count;
    }

    public int getTcp_ack_count() {
        return tcp_ack_count;
    }
    public void setTcp_ack_count(int tcp_ack_count) {
        this.tcp_ack_count = tcp_ack_count;
    }


    // DEPRECIADO — Para uso pelo desserializador
    public Flow() {}

    // Para uso pelo produtor(?)
    public Flow(String _flow_key,
                String _src_ip, String _dst_ip,
                int _src_port, int _dst_port,
                FlowProtocol _protocol, FlowDirection _dir,
                int _packet_count, int _byte_count,
                int _min_packet_size, int _max_packet_size,
                LocalDateTime _timestamp, double _mean_iat_us,
                int _tcp_syn_count, int _tcp_fin_count,
                int _tcp_rst_count, int _tcp_ack_count) {

        setFlow_key(_flow_key);
        setSrc_ip(_src_ip);
        setDst_ip(_dst_ip);
        setSrc_port(_src_port);
        setDst_port(_dst_port);
        setProtocol(_protocol);
        setDir(_dir);

        setPacket_count(_packet_count);
        setByte_count(_byte_count);
        setMin_packet_size(_min_packet_size);
        setMax_packet_size(_max_packet_size);

        setTimestamp(_timestamp);
        setMean_iat_us(_mean_iat_us);

        setTcp_syn_count(_tcp_syn_count);
        setTcp_fin_count(_tcp_fin_count);
        setTcp_rst_count(_tcp_rst_count);
        setTcp_ack_count(_tcp_ack_count);
    }

    // Para uso pelo parser
    public Flow(String _src_ip, String _dst_ip,
                int _src_port, int _dst_port,
                FlowProtocol _protocol, FlowDirection _dir,
                int _packet_count, int _byte_count,
                int _min_packet_size, int _max_packet_size,
                LocalDateTime _timestamp, double _mean_iat_us,
                int _tcp_syn_count, int _tcp_fin_count,
                int _tcp_rst_count, int _tcp_ack_count) {

        setSrc_ip(_src_ip);
        setDst_ip(_dst_ip);
        setSrc_port(_src_port);
        setDst_port(_dst_port);
        setProtocol(_protocol);
        setDir(_dir);

        setPacket_count(_packet_count);
        setByte_count(_byte_count);
        setMin_packet_size(_min_packet_size);
        setMax_packet_size(_max_packet_size);

        setTimestamp(_timestamp);
        setMean_iat_us(_mean_iat_us);

        setTcp_syn_count(_tcp_syn_count);
        setTcp_fin_count(_tcp_fin_count);
        setTcp_rst_count(_tcp_rst_count);
        setTcp_ack_count(_tcp_ack_count);

        StringBuilder sb = new StringBuilder();
        sb.append(_src_ip).append(':').append(_src_port).append('-');
        sb.append(_dst_ip).append(':').append(_dst_port).append('-');
        sb.append(_protocol.name());
        String _flow_key = sb.toString();
        setFlow_key(_flow_key);
    }
}
