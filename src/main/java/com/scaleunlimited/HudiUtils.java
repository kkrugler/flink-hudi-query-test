package com.scaleunlimited;

import java.util.function.Consumer;

import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.formats.avro.typeutils.AvroSchemaConverter;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.InputFormatSourceFunction;
import org.apache.flink.streaming.api.operators.OneInputStreamOperatorFactory;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.types.TypeInfoDataTypeConverter;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.configuration.OptionsInference;
import org.apache.hudi.keygen.CustomAvroKeyGenerator;
import org.apache.hudi.keygen.constant.KeyGeneratorType;
import org.apache.hudi.sink.utils.Pipelines;
import org.apache.hudi.source.StreamReadMonitoringFunction;
import org.apache.hudi.source.StreamReadOperator;
import org.apache.hudi.table.format.FilePathUtils;
import org.apache.hudi.table.format.mor.MergeOnReadInputFormat;
import org.apache.hudi.table.format.mor.MergeOnReadInputSplit;

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
                    .map(new ConvertToRowData(rowType))
                    .name("Convert Enriched to RowData");

            final boolean bounded = false;
            Pipelines.append(config, rowType, rowData, bounded);
        };
    }

    public static void setHudiConfig(Configuration config, String tablePath) {
        config.setString(FlinkOptions.PATH, tablePath);
        config.set(FlinkOptions.TABLE_NAME, HudiConstants.TABLE_NAME);
        config.set(FlinkOptions.TABLE_TYPE, HoodieTableType.COPY_ON_WRITE.name());
        config.setString(FlinkOptions.SOURCE_AVRO_SCHEMA, EnrichedRecord.getClassSchema().toString());

        config.setString(FlinkOptions.KEYGEN_TYPE, KeyGeneratorType.CUSTOM.name());
        config.setString(FlinkOptions.KEYGEN_CLASS_NAME, CustomAvroKeyGenerator.class.getName());
        
        config.setString(FlinkOptions.RECORD_KEY_FIELD, HudiConstants.RECORD_KEY_FIELD);
        config.setString(FlinkOptions.PARTITION_PATH_FIELD, HudiConstants.PARTITION_PATH_FIELD);
    }

    public static void setHudiWriteConfig(Configuration config, String tablePath, int writeParallelism) {
        setHudiConfig(config, tablePath);
        
        config.set(FlinkOptions.WRITE_TASKS, writeParallelism);
        config.set(FlinkOptions.OPERATION, WriteOperationType.INSERT.value());

        config.setBoolean(FlinkOptions.WRITE_BULK_INSERT_SORT_INPUT, false);
        config.setBoolean(FlinkOptions.WRITE_BULK_INSERT_SHUFFLE_INPUT, false);

        // Set all of these to 1MB, so we can run easily in our MiniCluster
        config.setDouble(FlinkOptions.WRITE_TASK_MAX_SIZE, 1);
        config.setInteger(FlinkOptions.WRITE_PARQUET_PAGE_SIZE, 1);
        config.setInteger(FlinkOptions.WRITE_PARQUET_BLOCK_SIZE, 1);
        config.setInteger(FlinkOptions.WRITE_PARQUET_MAX_FILE_SIZE, 1);
    }

    public static void setHudiReadConfig(Configuration config, String tablePath, int readParallelism) {
        setHudiConfig(config, tablePath);
        
        config.set(FlinkOptions.QUERY_TYPE, FlinkOptions.QUERY_TYPE_SNAPSHOT);
        config.setBoolean(FlinkOptions.READ_AS_STREAMING, true);
        config.set(FlinkOptions.READ_TASKS, readParallelism);
        config.set(FlinkOptions.READ_START_COMMIT, FlinkOptions.START_COMMIT_EARLIEST);
    }

}
