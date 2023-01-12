package com.scaleunlimited;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;

import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

@SuppressWarnings("serial")
public class AddEnrichments extends BroadcastProcessFunction<ExampleRecord, EnrichmentRecord, EnrichedRecord> {

    public static final MapStateDescriptor<EnrichmentRecord, Void> BROADCAST_STATE =
            new MapStateDescriptor<>(
                    "EnrichmentBroadcastState",
                    BasicTypeInfo.of(EnrichmentRecord.class),
                    TypeInformation.of(new TypeHint<Void>() {}));


    private transient Map<Integer, String> enrichmentMap;
    private transient MakePartitionKey partitioner;
    
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        
        partitioner = new MakePartitionKey();
    }
    
    @Override
    public void processBroadcastElement(EnrichmentRecord in,
            Context ctx,
            Collector<EnrichedRecord> out) throws Exception {
        if (enrichmentMap == null) {
            initEnrichmentMap(ctx.getBroadcastState(BROADCAST_STATE));
        }

        enrichmentMap.put(in.getKey(), in.getEnrichment());
    }

    @Override
    public void processElement(ExampleRecord in,
            ReadOnlyContext ctx,
            Collector<EnrichedRecord> out) throws Exception {
        if (enrichmentMap == null) {
            initEnrichmentMap(ctx.getBroadcastState(BROADCAST_STATE));
        }
        
        Integer data = in.getData();
        
        EnrichedRecord.Builder result = EnrichedRecord.newBuilder();
            result.setEventTime(in.getEventTime());
            result.setData(data);
            result.setEnrichment(enrichmentMap.get(data));
            result.setKey(String.format("%d|%d", in.getEventTime(), data));
            result.setPartition(partitioner.getKey(in.getEventTime()));
            
            out.collect(result.build());
    }

    private void initEnrichmentMap(ReadOnlyBroadcastState<EnrichmentRecord, Void> state) throws Exception {
        enrichmentMap = new HashMap<>();
        
        for (Map.Entry<EnrichmentRecord, Void> entry : state.immutableEntries()) {
            enrichmentMap.put(entry.getKey().getKey(), entry.getKey().getEnrichment());
        }
    }

    /**
     * Create partition name based on timestamp
     *
     */
    private static class MakePartitionKey {

        private SimpleDateFormat formatter;

        public MakePartitionKey() {
            formatter = new SimpleDateFormat(HudiConstants.PARTITION_OUTPUT_FORMAT);
            formatter.setTimeZone(TimeZone.getTimeZone("UTC"));
        }

        public String getKey(long timestamp) throws Exception {
            return formatter.format(new Date(timestamp));
        }
    }

}
