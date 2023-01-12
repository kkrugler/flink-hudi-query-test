package com.scaleunlimited;

import org.apache.avro.Schema;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.util.AvroSchemaConverter;

import com.google.common.annotations.VisibleForTesting;

@SuppressWarnings("serial")
public class ConvertToEnrichedRecord extends RichMapFunction<RowData, EnrichedRecord> {

    private String avroSchemaStr;
    private transient RowType rowType;

    public ConvertToEnrichedRecord(Configuration config) {
        avroSchemaStr = config.getString(FlinkOptions.SOURCE_AVRO_SCHEMA);
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        Schema avroSchema = new Schema.Parser().setValidate(true).parse(avroSchemaStr);
        DataType rowDataType = AvroSchemaConverter.convertToDataType(avroSchema);
        rowType = (RowType) rowDataType.getLogicalType();
    }

    @Override
    public EnrichedRecord map(RowData value) throws Exception {
        return convert(value, rowType);
    }

    @VisibleForTesting
    static EnrichedRecord convert(RowData value, RowType rt) throws Exception {

        return EnrichedRecord.newBuilder()
                .setPartition(value.getString(rt.getFieldIndex("partition")).toString())
                .setEventTime(value.getLong(rt.getFieldIndex("event_time")))
                .setData(value.getInt(rt.getFieldIndex("data")))
                .setEnrichment(getOptionalStringValue(value, rt, "enrichment"))
                .setKey(value.getString(rt.getFieldIndex("key")).toString())
                .build();
    }

    private static String getOptionalStringValue(RowData value, RowType rt, String fieldName) {
        int index = rt.getFieldIndex(fieldName);
        if (!value.isNullAt(index)) {
            return value.getString(index).toString();
        }
        
        return null;
    }
}
