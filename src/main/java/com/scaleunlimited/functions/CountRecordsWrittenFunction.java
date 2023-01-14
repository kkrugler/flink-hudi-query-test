package com.scaleunlimited.functions;

import java.util.concurrent.atomic.AtomicInteger;

import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.configuration.Configuration;

import com.scaleunlimited.EnrichedRecord;

/**
 * A no-op filter function that increments a shared counter, so we know when
 * we're done in our tests.
 *
 */
@SuppressWarnings("serial") 
public class CountRecordsWrittenFunction extends RichFilterFunction<EnrichedRecord> {
    
    // During testing, to get actual count
    private static final AtomicInteger WRITE_COUNT = new AtomicInteger();
    
    private final String counterName;
    
    private transient IntCounter recordCounter;
    
    public CountRecordsWrittenFunction(String counterName) {
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
        
        WRITE_COUNT.incrementAndGet();
        
        return true;
    }
    
    public static void resetCount() {
        WRITE_COUNT.set(0);
    }
    
    public static int getCount() {
        return WRITE_COUNT.get();
    }
}