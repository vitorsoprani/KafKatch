package br.ufes.edu;

import java.util.ArrayList;

import com.sshtools.twoslices.Toast;
import com.sshtools.twoslices.ToastType;

public class Notifier {
    private final ArrayList<Aggregate> aggregate_sample = new ArrayList<>();
    private int sample_max_size;

    // Estatísticas das amostras armazenadas
    private int bytes_inbound_sum, bytes_inbound_record,
                bytes_outbound_sum, bytes_outbound_record,
                unique_flow_sum, unique_flow_record;

    // Valores de alerta ao usuário
    private int bytes_inbound_limit,
                bytes_outbound_limit,
                unique_flow_limit;


    public int getSample_max_size() {
        return sample_max_size;
    }
    public void setSample_max_size(int sample_max_size) {
        this.sample_max_size = sample_max_size;
    }

    public ArrayList<Aggregate> getAggregate_sample() {
        return aggregate_sample;
    }
    public void addAggregate_sample(Aggregate newest) {
        // Começa por introduzir os valores da nova amostra
        aggregate_sample.add(newest);
        // TO-DO: Verificar recorde de bytes entrando e saindo
        if (newest.getFlow_key_count() > unique_flow_record)
            updateRecords();

        // TO-DO: Somar bytes entrando e saindo
        addUnique_flow_sum(newest.getFlow_key_count());


        // Se o tamanho máximo da amostra foi excedido, remove a mais antiga
        if (aggregate_sample.size() > sample_max_size) {
            Aggregate oldest = aggregate_sample.remove(0);
            // TO-DO: Verificar bytes entrando e saindo
            if (oldest.getFlow_key_count() >= unique_flow_record)
                updateRecords();
            
            // TO-DO: Subtrair bytes entrando e saindo
            addUnique_flow_sum(-oldest.getFlow_key_count());
        }
    }

    public int getBytes_inbound_sum() {
        return bytes_inbound_sum;
    }
    public void setBytes_inbound_sum(int bytes_inbound_sum) {
        this.bytes_inbound_sum = bytes_inbound_sum;
    }
    public void addBytes_inbound_sum(int amount) {
        bytes_inbound_sum += amount;
    }

    public int getBytes_inbound_record() {
        return bytes_inbound_record;
    }
    public void setBytes_inbound_record(int bytes_inbound_record) {
        this.bytes_inbound_record = bytes_inbound_record;
    }

    public int getBytes_outbound_sum() {
        return bytes_outbound_sum;
    }
    public void setBytes_outbound_sum(int bytes_outbound_sum) {
        this.bytes_outbound_sum = bytes_outbound_sum;
    }
    public void addBytes_outbound_sum(int amount) {
        bytes_outbound_sum += amount;
    }

    public int getBytes_outbound_record() {
        return bytes_outbound_record;
    }
    public void setBytes_outbound_record(int bytes_outbound_record) {
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

    public int getBytes_inbound_limit() {
        return bytes_inbound_limit;
    }
    public void setBytes_inbound_limit(int bytes_inbound_limit) {
        this.bytes_inbound_limit = bytes_inbound_limit;
    }

    public int getBytes_outbound_limit() {
        return bytes_outbound_limit;
    }
    public void setBytes_outbound_limit(int bytes_outbound_limit) {
        this.bytes_outbound_limit = bytes_outbound_limit;
    }

    public int getUnique_flow_limit() {
        return unique_flow_limit;
    }
    public void setUnique_flow_limit(int unique_flow_limit) {
        this.unique_flow_limit = unique_flow_limit;
    }


    public void updateRecords() {
        int bytes_inbound=-1, bytes_outbound=-1, unique_flows=-1;

        for (Aggregate a : aggregate_sample) {
            // TO-DO: Verificar bytes entrando e saindo

            if (a.getFlow_key_count() > unique_flows)
                unique_flows = a.getFlow_key_count();
        }

        setBytes_inbound_record(bytes_inbound);
        setBytes_outbound_record(bytes_outbound);
        setUnique_flow_record(unique_flows);
    }

    public void checkForAnomalies() {
        Aggregate a = aggregate_sample.get(aggregate_sample.size()-1);

        // TO-DO: Verificar valores de bytes

        if (a.getFlow_key_count() > getUnique_flow_limit())
            Toast.toast(ToastType.WARNING, "Alerta de número de conexões",
            "O último agregado registrou" + a.getFlow_key_count() + "conexões");
    }

    public Notifier(int _sample_max_size,
                    int _bytes_inbound_limit,
                    int _bytes_outbound_limit,
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
