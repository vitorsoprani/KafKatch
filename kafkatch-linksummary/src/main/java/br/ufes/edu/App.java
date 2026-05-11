package br.ufes.edu;

import java.time.LocalDateTime;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
        LocalDateTime timestamp_start = LocalDateTime.MIN,
                      timestamp_end = LocalDateTime.MAX;

        String flow_key = "key",
               src_ip = "src_ip", dst_ip = "dst_ip",
               src_port = "src_port", dst_port = "dst_port",
               protocol = "protocol";

        int packet_count = 2, byte_count = 20,
            min_packet_size = 8, max_packet_size = 12;

        
        
        Flow teste = new Flow(timestamp_start, timestamp_end,
                              flow_key, src_ip, dst_ip,
                              src_port, dst_port, protocol,
                              packet_count, byte_count,
                              min_packet_size, max_packet_size);

                              

        System.out.println("Teste de funcionamento básico:");

        System.out.println("Timestamp_start — entrada: " + timestamp_start + ", saida: " + teste.getTimestamp_start());
        System.out.println("Timestamp_end — entrada: " + timestamp_end + ", saida: " + teste.getTimestamp_end());

        System.out.println("Flow_key — entrada: " + flow_key + ", saida: " + teste.getFlow_key());
        System.out.println("Src_ip — entrada: " + src_ip + ", saida: " + teste.getSrc_ip());
        System.out.println("Dst_ip — entrada: " + dst_ip + ", saida: " + teste.getDst_ip());
        System.out.println("Src_port — entrada: " + src_port + ", saida: " + teste.getSrc_port());
        System.out.println("Dst_port — entrada: " + dst_port + ", saida: " + teste.getDst_port());
        System.out.println("Protocol — entrada: " + protocol + ", saida: " + teste.getProtocol());

        System.out.println("Packet_count — entrada: " + packet_count + ", saida: " + teste.getPacket_count());
        System.out.println("Byte_count — entrada: " + byte_count + ", saida: " + teste.getByte_count());
        System.out.println("Min_packet_size — entrada: " + min_packet_size + ", saida: " + teste.getMin_packet_size());
        System.out.println("Max_packet_size — entrada: " + max_packet_size + ", saida: " + teste.getMax_packet_size());
    }
}
