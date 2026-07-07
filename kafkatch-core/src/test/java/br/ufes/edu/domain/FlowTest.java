package br.ufes.edu.domain;

import java.time.LocalDateTime;

/**
 * Cliente para teste de unidade da classe Flow
 * 
 */
public class FlowTest {
    public static void main( String[] args )
    {
        String flow_key = "key",
               src_ip = "src_ip", dst_ip = "dst_ip";
        short  src_port = 0, dst_port = 0;
        FlowProtocol protocol = FlowProtocol.TCP;
        FlowDirection dir = FlowDirection.INBOUND;

        int   packet_count = 2, byte_count = 20;
        short min_packet_size = 8, max_packet_size = 12;

        LocalDateTime timestamp = LocalDateTime.now();
        double mean_iat_us = 2000.0;

        int tcp_syn_count = 1, tcp_fin_count = 1,
            tcp_rst_count = 0, tcp_ack_count = 0;

        
        
        Flow teste = new Flow(flow_key, src_ip, dst_ip,
                              src_port, dst_port, protocol, dir,
                              packet_count, byte_count,
                              min_packet_size, max_packet_size,
                              timestamp, mean_iat_us,
                              tcp_syn_count, tcp_fin_count,
                              tcp_rst_count, tcp_ack_count);

                              

        System.out.println("Teste de funcionamento básico:");

        System.out.println("Flow_key — entrada: " + flow_key + ", saida: " + teste.getFlow_key());
        System.out.println("Src_ip — entrada: " + src_ip + ", saida: " + teste.getSrc_ip());
        System.out.println("Dst_ip — entrada: " + dst_ip + ", saida: " + teste.getDst_ip());
        System.out.println("Src_port — entrada: " + src_port + ", saida: " + teste.getSrc_port());
        System.out.println("Dst_port — entrada: " + dst_port + ", saida: " + teste.getDst_port());
        System.out.println("Protocol — entrada: " + protocol.name() + ", saida: " + teste.getProtocol().name());
        System.out.println("Dir — entrada: " + dir.name() + ", saida: " + teste.getDir().name());

        System.out.println("Packet_count — entrada: " + packet_count + ", saida: " + teste.getPacket_count());
        System.out.println("Byte_count — entrada: " + byte_count + ", saida: " + teste.getByte_count());
        System.out.println("Min_packet_size — entrada: " + min_packet_size + ", saida: " + teste.getMin_packet_size());
        System.out.println("Max_packet_size — entrada: " + max_packet_size + ", saida: " + teste.getMax_packet_size());

        System.out.println("Timestamp — entrada: " + timestamp + ", saida: " + teste.getTimestamp());
        System.out.println("Mean_iat_us — entrada: " + mean_iat_us + ", saida: " + teste.getMean_iat_us());

        System.out.println("Tcp_syn_count — entrada: " + tcp_syn_count + ", saida: " + teste.getTcp_syn_count());
        System.out.println("Tcp_fin_count — entrada: " + tcp_fin_count + ", saida: " + teste.getTcp_fin_count());
        System.out.println("Tcp_rst_count — entrada: " + tcp_rst_count + ", saida: " + teste.getTcp_rst_count());
        System.out.println("Tcp_ack_count — entrada: " + tcp_ack_count + ", saida: " + teste.getTcp_ack_count());
    }
}
