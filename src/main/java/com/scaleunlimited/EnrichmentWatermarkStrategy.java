package com.scaleunlimited;

import org.apache.flink.api.common.eventtime.TimestampAssigner;
import org.apache.flink.api.common.eventtime.TimestampAssignerSupplier;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;

@SuppressWarnings("serial") 
public class EnrichmentWatermarkStrategy implements WatermarkStrategy<EnrichmentRecord> {

        @Override
        public TimestampAssigner<EnrichmentRecord> createTimestampAssigner(
                TimestampAssignerSupplier.Context ctx) {
            return new TimestampAssigner<EnrichmentRecord>() {

                @Override
                public long extractTimestamp(EnrichmentRecord element, long recordTimestamp) {
                    return element.getTimestamp();
                }
            };
        }

        @Override
        public WatermarkGenerator<EnrichmentRecord> createWatermarkGenerator(
                WatermarkGeneratorSupplier.Context ctx) {
            return WatermarkStrategy.<EnrichmentRecord>forMonotonousTimestamps()
                    .createWatermarkGenerator(ctx);
        }
}