package com.scaleunlimited;

import java.util.ArrayList;
import java.util.List;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("serial")
public class EnrichmentSource extends RichParallelSourceFunction<EnrichmentRecord>  {
    private static final Logger LOGGER = LoggerFactory.getLogger(EnrichmentSource.class);
    
    private List<EnrichmentRecord> enrichments;
    
    private transient volatile boolean running;
    private transient int curIndex;
    private transient int incBy;

    public EnrichmentSource(List<EnrichmentRecord> enrichments) {
        this.enrichments = enrichments;
    }
    
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        
        // Set up values such that we don't duplicate a value
        // from a source running in a different slot.
        curIndex = getRuntimeContext().getIndexOfThisSubtask();
        incBy = getRuntimeContext().getNumberOfParallelSubtasks();
    }

    @Override
    public void run(SourceContext<EnrichmentRecord> ctx) throws Exception {
        running = true;

        while (running) {
            if (curIndex < enrichments.size()) {
                ctx.collect(enrichments.get(curIndex));
                curIndex += incBy;
            } else {
                break;
            }
        }
        
        LOGGER.info("Exiting EnrichmentSource");
    }

    @Override
    public void cancel() {
        running = false;
    }
    
    public static List<EnrichmentRecord> makeEnrichments() {
        long timestamp = System.currentTimeMillis() - 1000;
        
        List<EnrichmentRecord> result = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            result.add(new EnrichmentRecord(timestamp + i, i, "Enrichment-" + i));
        }
        
        return result;
    }

    
}
