package com.bustrack.model;

import java.io.Serializable;

public class DedupMetrics implements Serializable {
    public String vehicle;
    public long   windowStartMs;
    public long   totalSeen;
    public long   duplicatesDropped;
    public double duplicateRate;
    public long   bloomFilterSizeBytes;
    public int    numHashFunctions;

    public DedupMetrics() {}
}
