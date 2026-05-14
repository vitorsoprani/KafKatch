package br.ufes.edu;

import java.io.StringReader;
import java.time.LocalDateTime;

import jakarta.json.Json;
import jakarta.json.JsonArray;
import jakarta.json.JsonObject;
import jakarta.json.JsonReader;

public class FlowParser {
    public static Flow readJson(String json, long kafka_timestamp) {
        JsonReader reader = Json.createReader(new StringReader(json));
        JsonObject object = reader.readObject();

        String src_ip = object.getString("src_ip"),
               dst_ip = object.getString("dst_ip");
        short  src_port = (short)object.getInt("src_port"),
               dst_port = (short)object.getInt("dst_port");
        FlowProtocol protocol = FlowProtocol.valueOf(object.getString("protocol"));
        FlowDirection dir = FlowDirection.valueOf(object.getString("dir"));

        int   packets = object.getInt("packets"),
              bytes = object.getInt("bytes");
        short min_size = (short)object.getInt("min_size"),
              max_size = (short)object.getInt("max_size");
        
        int nanoOfSecond = (int)(kafka_timestamp%1000) * 1000000;
        LocalDateTime timestamp = LocalDateTime.ofEpochSecond(kafka_timestamp/1000, nanoOfSecond, null);
        double mean_iat_us = object.getJsonNumber("mean_iat_us").doubleValue();

        JsonArray array = object.getJsonArray("tcp");
        JsonObject tcp = array.getJsonObject(0);
        int tcp_syn_count = tcp.getInt("syn"),
            tcp_fin_count = tcp.getInt("fin"),
            tcp_rst_count = tcp.getInt("rst"),
            tcp_ack_count = tcp.getInt("ack");

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
