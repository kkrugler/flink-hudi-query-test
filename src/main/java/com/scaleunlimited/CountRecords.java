package com.scaleunlimited;

import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.configuration.Configuration;

@SuppressWarnings("serial") 
public class CountRecords extends RichFilterFunction<EnrichedRecord> {
    
    private final String counterName;
    
    private transient IntCounter recordCounter;
    
    public CountRecords(String counterName) {
        this.counterName = counterName;
    }
    
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        
        recordCounter = getRuntimeContext().getIntCounter(counterName);
    }
    
    @Override
    public boolean filter(EnrichedRecord value) throws Exception {
        recordCounter.add(1);
        
        return true;
    }
}