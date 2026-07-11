package br.ufes.edu.domain;

import java.util.LinkedList;

/**
 * Mantém os últimos N valores (janela por contagem, não por tempo) para
 * calcular uma média móvel
 */
public class RollingStats {
    private LinkedList<Long> values = new LinkedList<>();
    private int maxSize;
    private long sum;

    // construtor vazio necessário para o Jackson
    public RollingStats() {}

    public RollingStats(int maxSize) {
        this.maxSize = maxSize;
    }

    public void add(long value) {
        values.addLast(value);
        sum += value;
        while (values.size() > maxSize) {
            sum -= values.removeFirst();
        }
    }

    public double average() {
        return values.isEmpty() ? 0.0 : (double) sum / values.size();
    }

    public int size() {
        return values.size();
    }

    public LinkedList<Long> getValues() {
        return values;
    }
    public void setValues(LinkedList<Long> values) {
        this.values = values;
    }

    public int getMaxSize() {
        return maxSize;
    }
    public void setMaxSize(int maxSize) {
        this.maxSize = maxSize;
    }

    public long getSum() {
        return sum;
    }
    public void setSum(long sum) {
        this.sum = sum;
    }
}