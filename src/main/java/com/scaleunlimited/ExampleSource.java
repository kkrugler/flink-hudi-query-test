package com.scaleunlimited;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * SourceFunction that generates a target number of ExampleRecord records.
 *
 */
@SuppressWarnings("serial") 
public class ExampleSource extends RichParallelSourceFunction<ExampleRecord> {
    private static final Logger LOGGER = LoggerFactory
            .getLogger(ExampleSource.class);
    
    private int maxValue;
    
    private transient volatile boolean running;
    private transient int curValue;
    private transient int incBy;
    
    public ExampleSource(int maxValue) {
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
    public void run(SourceContext<ExampleRecord> ctx) throws Exception {
        running = true;

        while (running && (curValue < maxValue)) {
            ctx.collect(new ExampleRecord(System.currentTimeMillis(), curValue));
            curValue += incBy;
        }
        
        LOGGER.info("Exiting ExampleSouce");
    }

    @Override
    public void cancel() {
        running = false;
    }
}