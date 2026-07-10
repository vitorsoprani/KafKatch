package br.ufes.edu;

import java.util.ArrayList;
import java.util.List;

import org.apache.kafka.streams.kstream.ValueTransformerWithKey;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

import br.ufes.edu.domain.AggregatedFlow;
import br.ufes.edu.domain.RollingStats;
import br.ufes.edu.domain.Warning;

/**
 * Para cada AggregatedFlow que chega, compara bytes_inbound, bytes_outbound
 * e unique_flow_count com a média dos últimos "windowSize" agregados
 * (guardada em state stores), e gera 0..N Warnings quando o valor atual
 * ultrapassa os limites relativos configurados.
 *
 * IMPORTANTE: a comparação é sempre feita com a média das amostras
 * ANTERIORES (o valor atual só é incorporado à janela depois da checagem),
 * então não há o problema de o próprio valor "puxar" a média para cima e
 * mascarar a anomalia.
 *
 * As 3 métricas são checadas de forma isolada (try/catch individual) para
 * que uma falha em uma delas (ex: state store corrompido) não impeça as
 * outras duas de continuarem funcionando nem derrube a StreamThread —
 * mesmo espírito defensivo do aggregate() em App.java, que ignora um Flow
 * nulo em vez de propagar a exceção.
 */
public class AnomalyDetectorTransformer implements ValueTransformerWithKey<String, AggregatedFlow, List<Warning>> {

    private final String storeInboundName;
    private final String storeOutboundName;
    private final String storeFlowsName;

    private final int windowSize;
    private final double ratioRegular; // ex: 1.2  -> 20% acima da média
    private final double ratioMedia;   // ex: 1.5  -> 50% acima da média
    private final double ratioAlta;    // ex: 2.0  -> 100% acima da média

    private KeyValueStore<String, RollingStats> inboundStore;
    private KeyValueStore<String, RollingStats> outboundStore;
    private KeyValueStore<String, RollingStats> flowsStore;

    public AnomalyDetectorTransformer(String storeInboundName, String storeOutboundName, String storeFlowsName,
                                       int windowSize, double ratioRegular, double ratioMedia, double ratioAlta) {
        this.storeInboundName = storeInboundName;
        this.storeOutboundName = storeOutboundName;
        this.storeFlowsName = storeFlowsName;
        this.windowSize = windowSize;
        this.ratioRegular = ratioRegular;
        this.ratioMedia = ratioMedia;
        this.ratioAlta = ratioAlta;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void init(ProcessorContext context) {
        try {
            inboundStore  = (KeyValueStore<String, RollingStats>) context.getStateStore(storeInboundName);
            outboundStore = (KeyValueStore<String, RollingStats>) context.getStateStore(storeOutboundName);
            flowsStore    = (KeyValueStore<String, RollingStats>) context.getStateStore(storeFlowsName);
            System.out.println("[INIT] AnomalyDetectorTransformer inicializado (stores: "
                    + storeInboundName + ", " + storeOutboundName + ", " + storeFlowsName + ")");
        } catch (Exception e) {
            // se os nomes passados aqui não baterem com os registrados via
            // builder.addStateStore(...) no OutputWatcher, isso falha em tempo de
            // execução (não de compilação) — por isso o log explícito ajuda a achar
            // o problema rápido.
            System.err.println("[INIT] Falha ao obter state stores: " + e.getMessage());
            throw e;
        }
    }

    @Override
    public List<Warning> transform(String key, AggregatedFlow value) {
        List<Warning> warnings = new ArrayList<>();

        if (value == null) {
            System.err.println("[TRANSFORM] AggregatedFlow nulo recebido, ignorando registro.");
            return warnings;
        }

        // se os eventos não tiverem uma chave significativa, tudo cai no mesmo "balde"
        String storeKey = (key != null) ? key : "global";

        try {
            warnings.addAll(check(inboundStore, storeKey, value.getBytes_inbound(), "bytes recebidos"));
        } catch (Exception e) {
            System.err.println("[CHECK-INBOUND] Erro ao processar bytes_inbound: " + e.getMessage());
        }

        try {
            warnings.addAll(check(outboundStore, storeKey, value.getBytes_outbound(), "bytes enviados"));
        } catch (Exception e) {
            System.err.println("[CHECK-OUTBOUND] Erro ao processar bytes_outbound: " + e.getMessage());
        }

        try {
            warnings.addAll(check(flowsStore, storeKey, value.getUnique_flow_count(), "conexões únicas"));
        } catch (Exception e) {
            System.err.println("[CHECK-FLOWS] Erro ao processar unique_flow_count: " + e.getMessage());
        }

        return warnings;
    }

    private List<Warning> check(KeyValueStore<String, RollingStats> store, String storeKey,
                                 long current, String label) {
        List<Warning> result = new ArrayList<>();

        RollingStats stats = store.get(storeKey);
        if (stats == null) {
            System.out.println("[STORE] Nenhum histórico para \"" + storeKey + "\" (" + label + "), iniciando novo.");
            stats = new RollingStats(windowSize);
        }

        // só avalia depois de ter uma quantidade mínima de amostras (evita alarme
        // falso logo no início, quando a média ainda não é representativa)
        if (stats.size() >= 3) {
            double average = stats.average();
            if (average > 0) {
                double ratio = current / average;
                int percentAcima = (int) Math.round((ratio - 1) * 100);

                System.out.println("[CHECK] " + label + " = " + current +
                        " | média (" + stats.size() + " amostras) = " + Math.round(average) +
                        " | razão = " + String.format("%.2f", ratio));

                String descricao = "O último agregado registrou " + current + " " + label + ", " +
                        percentAcima + "% acima da média de " + Math.round(average) +
                        " nas " + stats.size() + " amostras anteriores";

                if (ratio >= ratioAlta) {
                    result.add(new Warning("alta", descricao));
                } else if (ratio >= ratioMedia) {
                    result.add(new Warning("media", descricao));
                } else if (ratio >= ratioRegular) {
                    result.add(new Warning("regular", descricao));
                }
            }
        }

        // o valor atual só entra na janela DEPOIS da checagem acima — assim a
        // comparação é sempre contra a média das amostras anteriores, nunca
        // incluindo o próprio valor que está sendo avaliado.
        stats.add(current);
        store.put(storeKey, stats);

        return result;
    }

    @Override
    public void close() {
        System.out.println("[CLOSE] AnomalyDetectorTransformer finalizado.");
    }
}