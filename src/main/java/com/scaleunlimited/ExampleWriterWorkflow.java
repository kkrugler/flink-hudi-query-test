package com.scaleunlimited;

import java.util.function.Consumer;

import org.apache.flink.streaming.api.datastream.DataStream;

import com.scaleunlimited.functions.AddEnrichmentsFunction;
import com.scaleunlimited.functions.CountRecordsWrittenFunction;
import com.scaleunlimited.records.EnrichmentRecord;
import com.scaleunlimited.records.RawRecord;

public class ExampleWriterWorkflow {

    public static final String RECORD_COUNTER_NAME = "enriched-record-written-counter";

    private DataStream<RawRecord> input;
    private DataStream<EnrichmentRecord> enrichments;
    private Consumer<DataStream<EnrichedRecord>> output;
    
    public ExampleWriterWorkflow setInput(DataStream<RawRecord> input) {
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
                .connect(enrichments.broadcast(AddEnrichmentsFunction.BROADCAST_STATE))
                .process(new AddEnrichmentsFunction())
                .name("Add enrichments")
                .filter(new CountRecordsWrittenFunction(RECORD_COUNTER_NAME))
                .name("Count written records");
        
        output.accept(enriched);
                
        return this;
    }
    
}
