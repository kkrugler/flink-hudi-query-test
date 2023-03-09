package com.scaleunlimited;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.scaleunlimited.records.RawRecord;

/**
 * SourceFunction that generates a target number of raw (unenriched) records.
 *
 */
@SuppressWarnings("serial") 
public class RawSource extends RichParallelSourceFunction<RawRecord> {
    private static final Logger LOGGER = LoggerFactory
            .getLogger(RawSource.class);
    
    private int maxValue;
    
    private transient volatile boolean running;
    private transient int curValue;
    private transient int incBy;
    
    public RawSource(int maxValue) {
        this.maxValue = maxValue;
    }
    
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        
        // Set up values such that we don't duplicate a value
        // from a source running in a different slot.
        curValue = getRuntimeContext().getIndexOfThisSubtask();
        incBy = getRuntimeContext().getNumberOfParallelSubtasks();
    }
    
    @Override
    public void run(SourceContext<RawRecord> ctx) throws Exception {
        running = true;
        
        int numRecords = 0;
        while (running && (curValue < maxValue)) {
            long eventTime = System.currentTimeMillis();
            // Each record has a time that's backed up by
            // 0...4 minutes.
            eventTime -= (numRecords % 5) * 60 * 1000L;
            
            ctx.collect(new RawRecord(eventTime, curValue));
            curValue += incBy;
            numRecords++;
        }
        
        LOGGER.info("Exiting ExampleSouce");
    }

    @Override
    public void cancel() {
        running = false;
    }
}