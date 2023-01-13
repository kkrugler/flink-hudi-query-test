package com.scaleunlimited;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;

public class ExampleReaderWorkflow {

    public static final String RECORD_COUNTER_NAME = "enriched-record-read-counter";
    
    private DataStream<EnrichedRecord> input;
    
    public ExampleReaderWorkflow setInput(DataStream<EnrichedRecord> input) {
        this.input = input;
        return this;
    }
    
    public void build() {
        input.filter(new CountRecordsRead(RECORD_COUNTER_NAME))
        .name("Count read record")
        .addSink(new DiscardingSink<>())
        .name("End of reader workflow");
    }
}
