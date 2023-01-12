package com.scaleunlimited;

import java.io.Serializable;

@SuppressWarnings("serial")
public class EnrichmentRecord implements Serializable {

    private long timestamp;
    private int key;
    private String enrichment;
    
    public EnrichmentRecord() {}

    public EnrichmentRecord(long timestamp, int key, String enrichment) {
        this.timestamp = timestamp;
        this.key = key;
        this.enrichment = enrichment;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public int getKey() {
        return key;
    }

    public void setKey(int key) {
        this.key = key;
    }

    public String getEnrichment() {
        return enrichment;
    }

    public void setEnrichment(String enrichment) {
        this.enrichment = enrichment;
    }
    
}
