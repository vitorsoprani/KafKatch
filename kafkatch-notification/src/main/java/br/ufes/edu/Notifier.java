package br.ufes.edu;

import java.util.ArrayList;
import java.util.DoubleSummaryStatistics;

import com.sshtools.twoslices.Toast;
import com.sshtools.twoslices.ToastType;

public class Notifier {
    private final ArrayList<AggregatedFlow> aggregate_sample = new ArrayList<>();
    private int sample_max_size;

    // Estatísticas das amostras armazenadas
    private DoubleSummaryStatistics bytes_inbound_stats,
                                    bytes_outbound_stats,
                                    unique_flow_stats;

    // Valores de alerta ao usuário
    private long bytes_inbound_limit,
                 bytes_outbound_limit;
    private int  unique_flow_limit;


    public int getSample_size() {
        return aggregate_sample.size();
    }
    
    public int getSample_max_size() {
        return sample_max_size;
    }
    public void setSample_max_size(int sample_max_size) {
        this.sample_max_size = sample_max_size;
    }

    public ArrayList<AggregatedFlow> getAggregate_sample() {
        return aggregate_sample;
    }
    public void addAggregate_sample(AggregatedFlow newest) {
        aggregate_sample.add(newest);
        
        if (getSample_size() > getSample_max_size())
            aggregate_sample.remove(0);

        updateStats();
    }

    public long getBytes_inbound_sum() {
        return (long)bytes_inbound_stats.getSum();
    }

    public long getBytes_inbound_record() {
        return (long)bytes_inbound_stats.getMax();
    }

    public long getBytes_outbound_sum() {
        return (long)bytes_outbound_stats.getSum();
    }

    public long getBytes_outbound_record() {
        return (long)bytes_outbound_stats.getMax();
    }

    public int getUnique_flow_sum() {
        return (int)unique_flow_stats.getSum();
    }

    public int getUnique_flow_record() {
        return (int)unique_flow_stats.getMax();
    }

    public long getBytes_inbound_limit() {
        return bytes_inbound_limit;
    }
    public void setBytes_inbound_limit(long bytes_inbound_limit) {
        this.bytes_inbound_limit = bytes_inbound_limit;
    }

    public long getBytes_outbound_limit() {
        return bytes_outbound_limit;
    }
    public void setBytes_outbound_limit(long bytes_outbound_limit) {
        this.bytes_outbound_limit = bytes_outbound_limit;
    }

    public int getUnique_flow_limit() {
        return unique_flow_limit;
    }
    public void setUnique_flow_limit(int unique_flow_limit) {
        this.unique_flow_limit = unique_flow_limit;
    }


    public void updateStats() {
        DoubleSummaryStatistics bytes_inbound = new DoubleSummaryStatistics();
        aggregate_sample.forEach(a -> bytes_inbound_stats.accept(a.getBytes_inbound()));
        bytes_inbound_stats = bytes_inbound;

        DoubleSummaryStatistics bytes_outbound = new DoubleSummaryStatistics();
        aggregate_sample.forEach(a -> bytes_outbound_stats.accept(a.getBytes_outbound()));
        bytes_outbound_stats = bytes_outbound;

        DoubleSummaryStatistics unique_flow = new DoubleSummaryStatistics();
        aggregate_sample.forEach(a -> unique_flow.accept(a.getUnique_flow_count()));
        unique_flow_stats = unique_flow;
    }

    public void checkForAnomalies() {
        AggregatedFlow a = aggregate_sample.get(aggregate_sample.size()-1);

        if (a.getBytes_inbound() > getBytes_inbound_limit())
            Toast.toast(ToastType.WARNING, "Alerta de bytes recebidos",
            "O último agregado registrou " + a.getBytes_inbound() + " bytes recebidos");

        if (a.getBytes_outbound() > getBytes_outbound_limit())
            Toast.toast(ToastType.WARNING, "Alerta de bytes enviados",
            "O último agregado registrou " + a.getBytes_outbound() + " bytes enviados");

        if (a.getUnique_flow_count() > getUnique_flow_limit())
            Toast.toast(ToastType.WARNING, "Alerta de número de conexões",
            "O último agregado registrou " + a.getUnique_flow_count() + " conexões");
    }

    public Notifier(int _sample_max_size,
                    long _bytes_inbound_limit,
                    long _bytes_outbound_limit,
                    int _unique_flows_limit) {
        setSample_max_size(_sample_max_size);

        // Criando um objeto que apenas registra a ausência de valores
        // caso um método seja chamado antes de adicionar um Aggregate
        bytes_inbound_stats = bytes_outbound_stats = unique_flow_stats = new DoubleSummaryStatistics();

        setBytes_inbound_limit(_bytes_inbound_limit);
        setBytes_outbound_limit(_bytes_outbound_limit);
        setUnique_flow_limit(_unique_flows_limit);
    }
}
