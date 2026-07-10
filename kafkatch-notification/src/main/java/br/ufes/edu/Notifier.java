package br.ufes.edu;

import java.util.ArrayList;

import com.sshtools.twoslices.Toast;
import com.sshtools.twoslices.ToastType;

import br.ufes.edu.domain.AggregatedFlow;

public class Notifier {
    private final ArrayList<AggregatedFlow> aggregate_sample = new ArrayList<>();
    private int sample_max_size;

    // Estatísticas das amostras armazenadas
    private long bytes_inbound_sum, bytes_inbound_record,
                 bytes_outbound_sum, bytes_outbound_record;
    private int  unique_flow_sum, unique_flow_record;

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
        // Começa por introduzir os valores da nova amostra
        aggregate_sample.add(newest);
        if (newest.getBytes_inbound() > getBytes_inbound_record())
            setBytes_inbound_record(newest.getBytes_inbound());

        if (newest.getBytes_outbound() > getBytes_outbound_record())
            setBytes_outbound_record(newest.getBytes_outbound());

        if (newest.getUnique_flow_count() > getUnique_flow_record())
            setUnique_flow_record(newest.getUnique_flow_count());

        addBytes_inbound_sum(newest.getBytes_inbound());
        addBytes_outbound_sum(newest.getBytes_outbound());
        addUnique_flow_sum(newest.getUnique_flow_count());


        // Se o tamanho máximo da amostra foi excedido, remove a mais antiga
        if (getSample_size() > getSample_max_size()) {
            AggregatedFlow oldest = aggregate_sample.remove(0);

            // Se a mais antiga era recordista, deve-se buscar outra que a substitua
            if ((oldest.getBytes_inbound() >= getBytes_inbound_record()) ||
                (oldest.getBytes_outbound() >= getBytes_outbound_record()) ||
                (oldest.getUnique_flow_count() >= getUnique_flow_record()))
                updateRecords();
            
            addBytes_inbound_sum(-oldest.getBytes_inbound());
            addBytes_outbound_sum(-oldest.getBytes_outbound());
            addUnique_flow_sum(-oldest.getUnique_flow_count());
        }
    }

    public long getBytes_inbound_sum() {
        return bytes_inbound_sum;
    }
    public void setBytes_inbound_sum(long bytes_inbound_sum) {
        this.bytes_inbound_sum = bytes_inbound_sum;
    }
    public void addBytes_inbound_sum(long amount) {
        bytes_inbound_sum += amount;
    }

    public long getBytes_inbound_record() {
        return bytes_inbound_record;
    }
    public void setBytes_inbound_record(long bytes_inbound_record) {
        this.bytes_inbound_record = bytes_inbound_record;
    }

    public long getBytes_outbound_sum() {
        return bytes_outbound_sum;
    }
    public void setBytes_outbound_sum(long bytes_outbound_sum) {
        this.bytes_outbound_sum = bytes_outbound_sum;
    }
    public void addBytes_outbound_sum(long amount) {
        bytes_outbound_sum += amount;
    }

    public long getBytes_outbound_record() {
        return bytes_outbound_record;
    }
    public void setBytes_outbound_record(long bytes_outbound_record) {
        this.bytes_outbound_record = bytes_outbound_record;
    }

    public int getUnique_flow_sum() {
        return unique_flow_sum;
    }
    public void setUnique_flow_sum(int unique_flow_sum) {
        this.unique_flow_sum = unique_flow_sum;
    }
    public void addUnique_flow_sum(int amount) {
        unique_flow_sum += amount;
    }

    public int getUnique_flow_record() {
        return unique_flow_record;
    }
    public void setUnique_flow_record(int unique_flow_record) {
        this.unique_flow_record = unique_flow_record;
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


    public void updateRecords() {
        long bytes_inbound=-1, bytes_outbound=-1;
        int  unique_flows=-1;

        for (AggregatedFlow a : aggregate_sample) {
            if (a.getBytes_inbound() > bytes_inbound)
                bytes_inbound = a.getBytes_inbound();

            if (a.getBytes_outbound() > bytes_outbound)
                bytes_outbound = a.getBytes_outbound();

            if (a.getUnique_flow_count() > unique_flows)
                unique_flows = a.getUnique_flow_count();
        }

        setBytes_inbound_record(bytes_inbound);
        setBytes_outbound_record(bytes_outbound);
        setUnique_flow_record(unique_flows);
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

        setBytes_inbound_record(-1);
        setBytes_inbound_sum(0);
        setBytes_outbound_record(-1);
        setBytes_outbound_sum(0);
        setUnique_flow_record(-1);
        setUnique_flow_sum(0);

        setBytes_inbound_limit(_bytes_inbound_limit);
        setBytes_outbound_limit(_bytes_outbound_limit);
        setUnique_flow_limit(_unique_flows_limit);
    }
}
