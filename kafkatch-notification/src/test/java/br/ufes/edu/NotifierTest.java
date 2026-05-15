package br.ufes.edu;

import java.time.LocalDateTime;

public class NotifierTest {
    public static void main(String[] args) throws Exception {
        Notifier notif = new Notifier(5, 100, 100, 10);

        Flow f = new Flow(null, null, 0, 0, FlowProtocol.TCP, FlowDirection.INBOUND, 10, 1000, 50, 200, LocalDateTime.now(), 10, 10, 0, 0, 0);
        AggregatedFlow a = new AggregatedFlow(f.getTimestamp());
        a.addFlow(f);
        
        notif.addAggregate_sample(a);
        notif.checkForAnomalies();
    }
}
