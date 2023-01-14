package com.scaleunlimited;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.formats.avro.typeutils.AvroSchemaConverter;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.ProviderContext;
import org.apache.flink.table.connector.source.DataStreamScanProvider;
import org.apache.flink.table.connector.source.DynamicTableSource.DataStructureConverter;
import org.apache.flink.table.connector.source.ScanTableSource.ScanContext;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.keygen.CustomAvroKeyGenerator;
import org.apache.hudi.keygen.constant.KeyGeneratorType;
import org.apache.hudi.sink.utils.Pipelines;
import org.apache.hudi.table.HoodieTableSource;

import com.scaleunlimited.functions.ConvertRawToRowDataFunction;
import com.scaleunlimited.functions.ConvertRowDataToEnrichedFunction;

/**
 * Utility routines for creating input or output DataStreams using a
 * Hudi table as the source or sink.
 *
 */
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
                    .map(new ConvertRawToRowDataFunction(rowType))
                    .name("Convert Enriched to RowData");

            final boolean bounded = false;
            Pipelines.append(config, rowType, rowData, bounded);
        };
    }

    /**
     * This is the key method that takes our streaming envirnment and creates an input
     * stream of EnrichedRecord records. This stream is incremental, in that it will
     * never terminate, and will return additional records whenever a new snapshot is
     * found in the Hudi table.
     * 
     * @param env
     * @param config
     * @param tableDir
     * @return
     */
    public static DataStream<EnrichedRecord> makeHudiInput(StreamExecutionEnvironment env,
            Configuration config, String tableDir, int readParallelism) {
        
        HudiUtils.setHudiReadConfig(config, tableDir, readParallelism);

        DataType rowDataType = org.apache.hudi.util.AvroSchemaConverter.convertToDataType(EnrichedRecord.SCHEMA$);
        String[] partitionKeys = new String[] {HudiConstants.PARTITION_PATH_FIELD};
        HoodieTableSource hoodieTableSource =
                new HoodieTableSource(
                        makeResolvedSchema(rowDataType),
                        new org.apache.hadoop.fs.Path(tableDir),
                        Arrays.asList(partitionKeys),
                        "no_partition",
                        config);

        // If we want to get an InputFormat that works properly (scans the table continuously) we
        // have to use the undocumented HoodieTableSource.getScanRuntimeProvider() method.
        // We're assuming the ScanContext is never used (which is the case for the current code).
        DataStreamScanProvider dsa = (DataStreamScanProvider)hoodieTableSource.getScanRuntimeProvider(new ScanContext() {
            
            @Override
            public <T> TypeInformation<T> createTypeInformation(LogicalType producedLogicalType) {
                throw new RuntimeException("ScanContext.createTypeInformation not implemented!");
            }
            
            @Override
            public <T> TypeInformation<T> createTypeInformation(DataType producedDataType) {
                throw new RuntimeException("ScanContext.createTypeInformation not implemented!");
            }
            
            @Override
            public DataStructureConverter createDataStructureConverter(DataType producedDataType) {
                throw new RuntimeException("ScanContext.createDataStructureConverter not implemented!");
            }
        });
        
        ProviderContext pc = new ProviderContext() {
            
            @Override
            public Optional<String> generateUid(String name) {
                return Optional.of(name + "-ExampleWorkflow");
            }
        };
        
        DataStream<EnrichedRecord> result = dsa.produceDataStream(pc, env)
                .map(new ConvertRowDataToEnrichedFunction(config))
                .name("Convert RowData to EnrichedRecord");

        return result;
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

    private static ResolvedSchema makeResolvedSchema(DataType rowDataType) {
        RowType rowType = (RowType) rowDataType.getLogicalType();
        List<String> names = rowType.getFieldNames();
        List<DataType> types = rowDataType.getChildren();
        List<Column> columns =
                IntStream.range(0, names.size())
                        .mapToObj(idx -> Column.physical(names.get(idx), types.get(idx)))
                        .collect(Collectors.toList());
        return ResolvedSchema.of(columns);
    }

}
