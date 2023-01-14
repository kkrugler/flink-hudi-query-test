package com.scaleunlimited.functions;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.hudi.util.AvroToRowDataConverters;
import org.apache.hudi.util.AvroToRowDataConverters.AvroToRowDataConverter;

import com.scaleunlimited.EnrichedRecord;

/**
 * Convert our EnrichedRecord to the RowData type that Hudi needs.
 *
 */
@SuppressWarnings("serial")
public class ConvertRawToRowDataFunction extends RichMapFunction<EnrichedRecord, RowData> {

    private RowType rowType;
    private transient AvroToRowDataConverter converter;

    public ConvertRawToRowDataFunction(RowType rowType) {
        this.rowType = rowType;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        // For better performance we could do a manual conversion, but
        // use the (easier) built-in support.
        converter = AvroToRowDataConverters.createRowConverter(rowType);
    }

    @Override
    public RowData map(EnrichedRecord in) throws Exception {
        return (RowData) converter.convert(in);
    }
}
