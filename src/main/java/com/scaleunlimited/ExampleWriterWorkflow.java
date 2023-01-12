package com.scaleunlimited;

import java.util.function.Consumer;

import org.apache.flink.streaming.api.datastream.DataStream;

public class ExampleWriterWorkflow {

    public static final String RECORD_COUNTER_NAME = "enriched-record-write-counter";
    
    private DataStream<ExampleRecord> input;
    private DataStream<EnrichmentRecord> enrichments;
    private Consumer<DataStream<EnrichedRecord>> output;
    
    public ExampleWriterWorkflow setInput(DataStream<ExampleRecord> input) {
        this.input = input;
        return this;
    }
    
    public ExampleWriterWorkflow setEnrichments(DataStream<EnrichmentRecord> enrichments) {
        this.enrichments = enrichments;
        return this;
    }

    public ExampleWriterWorkflow setOutput(Consumer<DataStream<EnrichedRecord>> output) {
        this.output = output;
        return this;
    }

    public ExampleWriterWorkflow build() {
        DataStream<EnrichedRecord> enriched = input
                .connect(enrichments.broadcast(AddEnrichments.BROADCAST_STATE))
                .process(new AddEnrichments())
                .filter(new CountRecords(RECORD_COUNTER_NAME));
        
        output.accept(enriched);
                
        return this;
    }
    
}