package com.scaleunlimited;

import java.util.function.Consumer;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.formats.avro.typeutils.AvroSchemaConverter;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.keygen.CustomAvroKeyGenerator;
import org.apache.hudi.keygen.constant.KeyGeneratorType;
import org.apache.hudi.sink.utils.Pipelines;

public class HudiUtils {

    /**
     * Set up a Hudi pipeline to consume EnrichedRecords from a DataStream
     * 
     * @param env
     * @param config
     * @param outputPath
     * @return
     */
    public static Consumer<DataStream<EnrichedRecord>> makeHudiOutput(StreamExecutionEnvironment env,
            Configuration config, String outputPath, int writeParallelism) {
        
        setHudiWriteConfig(config, outputPath, writeParallelism);

        DataType rowDataType =
                AvroSchemaConverter.convertToDataType(EnrichedRecord.SCHEMA$.toString());
        RowType rowType = (RowType) rowDataType.getLogicalType();

        return (stream) -> {
            DataStream<RowData> rowData = stream
                    .map(new ConvertToRowData(rowType));

            Pipelines.bulkInsert(config, rowType, rowData);
        };
    }

    public static void setHudiWriteConfig(Configuration config, String tablePath, int writeParallelism) {
        config.setString(FlinkOptions.PATH, tablePath);
        config.set(FlinkOptions.WRITE_TASKS, writeParallelism);
        config.set(FlinkOptions.TABLE_NAME, HudiConstants.TABLE_NAME);
        config.set(FlinkOptions.TABLE_TYPE, HoodieTableType.MERGE_ON_READ.name());
        config.set(FlinkOptions.OPERATION, WriteOperationType.BULK_INSERT.value());
        config.setString(FlinkOptions.SOURCE_AVRO_SCHEMA, EnrichedRecord.getClassSchema().toString());

        config.setBoolean(FlinkOptions.WRITE_BULK_INSERT_SORT_INPUT, false);
        config.setBoolean(FlinkOptions.WRITE_BULK_INSERT_SHUFFLE_INPUT, false);

        config.setString(FlinkOptions.KEYGEN_TYPE, KeyGeneratorType.CUSTOM.name());
        config.setString(FlinkOptions.KEYGEN_CLASS_NAME, CustomAvroKeyGenerator.class.getName());
        
        config.setString(FlinkOptions.RECORD_KEY_FIELD, HudiConstants.RECORD_KEY_FIELD);
        config.setString(FlinkOptions.PARTITION_PATH_FIELD, HudiConstants.PARTITION_PATH_FIELD);

        // Set all of these to 1MB, so we can run easily in our MiniCluster
        config.setDouble(FlinkOptions.WRITE_TASK_MAX_SIZE, 1);
        config.setInteger(FlinkOptions.WRITE_PARQUET_PAGE_SIZE, 1);
        config.setInteger(FlinkOptions.WRITE_PARQUET_BLOCK_SIZE, 1);
        config.setInteger(FlinkOptions.WRITE_PARQUET_MAX_FILE_SIZE, 1);
    }

}
