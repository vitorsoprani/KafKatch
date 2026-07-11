package br.ufes.edu.domain;

import java.io.Serializable;
import java.util.LongSummaryStatistics;

// CLASSE DEPRECIADA — USE RollingStats.java
public class LatestStat implements Serializable {
    private long   sample_size,
                   latest_entry;
    private double stats_average,
                   entry_to_average_ratio;

    
    public long getSample_size() {
        return sample_size;
    }
    public void setSample_size(long sample_size) {
        this.sample_size = sample_size;
    }

    public long getLatest_entry() {
        return latest_entry;
    }
    public void setLatest_entry(long latest_entry) {
        this.latest_entry = latest_entry;
    }

    public double getStats_average() {
        return stats_average;
    }
    public void setStats_average(double stats_average) {
        this.stats_average = stats_average;
    }

    public double getEntry_to_average_ratio() {
        return entry_to_average_ratio;
    }
    public void setEntry_to_average_ratio(double entry_to_average_ratio) {
        this.entry_to_average_ratio = entry_to_average_ratio;
    }


    public LatestStat(long latest_entry, LongSummaryStatistics stats) {
        setSample_size(stats.getCount());
        setLatest_entry(latest_entry);
        setStats_average(stats.getAverage());
        setEntry_to_average_ratio((double)latest_entry / stats.getAverage());
    }
}
