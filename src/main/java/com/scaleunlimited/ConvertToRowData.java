package com.scaleunlimited;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.hudi.util.AvroToRowDataConverters;
import org.apache.hudi.util.AvroToRowDataConverters.AvroToRowDataConverter;

@SuppressWarnings("serial")
public class ConvertToRowData extends RichMapFunction<EnrichedRecord, RowData> {

    private RowType rowType;
    private transient AvroToRowDataConverter converter;

    public ConvertToRowData(RowType rowType) {
        this.rowType = rowType;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        converter = AvroToRowDataConverters.createRowConverter(rowType);
    }

    @Override
    public RowData map(EnrichedRecord in) throws Exception {
        return (RowData) converter.convert(in);
    }
}
