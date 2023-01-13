package com.scaleunlimited;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.commons.io.FileUtils;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.ProviderContext;
import org.apache.flink.table.connector.source.DataStreamScanProvider;
import org.apache.flink.table.connector.source.DynamicTableSource.DataStructureConverter;
import org.apache.flink.table.connector.source.ScanTableSource.ScanContext;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.hudi.table.HoodieTableSource;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExampleWorkflowTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(ExampleWorkflowTest.class);
    
    private static final int PARALLELISM = 2;
    private static final int MAX_PARALLELISM = 8192;
    private static final long CHECKPOINT_INTERVAL_MS = 5 * 1000L;

    private static final int NUM_RESULTS = 100_000;
    
    @Test
    void testHudiQueryOfExistingTable() throws Exception {
        File testDir = getTestDir("testHudiQueryOfExistingTable", true);
        File inputDir = writeHudiFiles(testDir);
        
        Configuration conf = new Configuration();
        StreamExecutionEnvironment env = makeExecutionEnvironment(testDir, conf);

        new ExampleReaderWorkflow()
            .setInput(makeHudiInput(env, conf, inputDir.getAbsolutePath()))
            .build();
    
        CountRecordsRead.resetCount();
        JobClient readerClient = env.executeAsync("reader workflow");
        
        int readCount = 0;
        // Set max time based on number of records, and at least one checkpoint
        long maxTime = System.currentTimeMillis() + Math.max(CHECKPOINT_INTERVAL_MS * 2, NUM_RESULTS / 10);
        while ((readCount < NUM_RESULTS) && (System.currentTimeMillis() < maxTime)) {
            Thread.sleep(100);
            readCount = CountRecordsRead.getCount();
        }
        
        readerClient.cancel();
        
        assertEquals(NUM_RESULTS, readCount);
    }
    
    private File writeHudiFiles(File testDir) throws Exception {
        Configuration conf = new Configuration();
        StreamExecutionEnvironment env = makeExecutionEnvironment(testDir, conf);
        
        DataStream<ExampleRecord> input = env
                .addSource(new ExampleSource(NUM_RESULTS), "Example Source")
                .assignTimestampsAndWatermarks(WatermarkStrategy.noWatermarks())
                .setParallelism(PARALLELISM);
        
        List<EnrichmentRecord> enrichments = EnrichmentSource.makeEnrichments();

        File outputDir = new File(testDir, "hudi-table");
        
        new ExampleWriterWorkflow()
            .setInput(input)
            .setEnrichments(env.addSource(new EnrichmentSource(enrichments)))
            .setOutput(HudiUtils.makeHudiOutput(env, conf, outputDir.getAbsolutePath(), PARALLELISM))
            .build();

        env.execute("writer workflow");
        
        return outputDir;
    }
    
    @Test
    void testHudiWriteAndIncrementalQuery() throws Exception {
        File testDir = getTestDir("testHudiWriteAndIncrementalQuery", true);
        File outputDir = new File(testDir, "hudi-table");
        
        Configuration conf = new Configuration();
        StreamExecutionEnvironment env = makeExecutionEnvironment(testDir, conf);
        
        DataStream<ExampleRecord> input = env
                .addSource(new ExampleSource(NUM_RESULTS), "Example Source")
                .assignTimestampsAndWatermarks(WatermarkStrategy.noWatermarks())
                .setParallelism(PARALLELISM);
        
        List<EnrichmentRecord> enrichments = EnrichmentSource.makeEnrichments();

        new ExampleWriterWorkflow()
            .setInput(input)
            .setEnrichments(env.addSource(new EnrichmentSource(enrichments)))
            .setOutput(HudiUtils.makeHudiOutput(env, conf, outputDir.getAbsolutePath(), PARALLELISM))
            .build();
        
        LOGGER.info("Starting execution of writer workflow...");
        env.executeAsync("writer workflow");
        
        // Wait for table to be created, before starting up reader
        // If we don't do this, the reader says that no table exists
//        LOGGER.info("Waiting for Hudi table to exist...");
//        Thread.sleep(CHECKPOINT_INTERVAL_MS * 2);
        
        StreamExecutionEnvironment env2 = makeExecutionEnvironment(testDir, conf);
        
        new ExampleReaderWorkflow()
            .setInput(makeHudiInput(env2, conf, outputDir.getAbsolutePath()))
            .build();
        
        LOGGER.info("Starting execution of reader workflow...");
        CountRecordsRead.resetCount();
        JobClient readerClient = env2.executeAsync("reader workflow");
        
        int readCount = 0;
        // Set max time based on number of records, and at least one checkpoint
        long maxTime = System.currentTimeMillis() + Math.max(CHECKPOINT_INTERVAL_MS * 3, NUM_RESULTS / 10);
        while ((readCount < NUM_RESULTS) && (System.currentTimeMillis() < maxTime)) {
            Thread.sleep(100);
            readCount = CountRecordsRead.getCount();
        }
        
        readerClient.cancel();
        
        assertEquals(NUM_RESULTS, readCount);
    }

    private StreamExecutionEnvironment makeExecutionEnvironment(File testDir, Configuration conf) throws Exception {
        File logFile = new File(testDir, "flink-logs");
        logFile.mkdirs();
        System.setProperty("log.file", logFile.getAbsolutePath());

        // Set up for final checkpoint (and thus final commit of inflight data) when workflow ends
        conf.setBoolean(
                ExecutionCheckpointingOptions.ENABLE_CHECKPOINTS_AFTER_TASKS_FINISH, true);

        MiniClusterWithClientResource flinkCluster = 

                new MiniClusterWithClientResource(
                        new MiniClusterResourceConfiguration.Builder()
                            .setConfiguration(conf)
                            // Set up with 2x the number of slots we need, so we can run two jobs at once.
                            .setNumberSlotsPerTaskManager(PARALLELISM * 2)
                            .setNumberTaskManagers(1)
                            .build());

        flinkCluster.before();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(PARALLELISM);
        env.setMaxParallelism(MAX_PARALLELISM);
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        env.enableCheckpointing(CHECKPOINT_INTERVAL_MS, CheckpointingMode.EXACTLY_ONCE);
        env.setStateBackend(new HashMapStateBackend());
        env.getConfig().enableObjectReuse();
        return env;
    }

    private DataStream<EnrichedRecord> makeHudiInput(StreamExecutionEnvironment env,
            Configuration config, String tableDir) {
        
        HudiUtils.setHudiReadConfig(config, tableDir, PARALLELISM);

        DataType rowDataType = org.apache.hudi.util.AvroSchemaConverter.convertToDataType(EnrichedRecord.SCHEMA$);
        String[] partitionKeys = new String[] {HudiConstants.PARTITION_PATH_FIELD};
        HoodieTableSource hoodieTableSource =
                new HoodieTableSource(
                        makeResolvedSchema(rowDataType),
                        new org.apache.hadoop.fs.Path(tableDir),
                        Arrays.asList(partitionKeys),
                        "no_partition",
                        config);

        DataStreamScanProvider dsa = (DataStreamScanProvider)hoodieTableSource.getScanRuntimeProvider(new ScanContext() {
            
            @Override
            public <T> TypeInformation<T> createTypeInformation(LogicalType producedLogicalType) {
                // TODO Auto-generated method stub
                return null;
            }
            
            @Override
            public <T> TypeInformation<T> createTypeInformation(DataType producedDataType) {
                // TODO Auto-generated method stub
                return null;
            }
            
            @Override
            public DataStructureConverter createDataStructureConverter(DataType producedDataType) {
                // TODO Auto-generated method stub
                return null;
            }
        });
        
        ProviderContext pc = new ProviderContext() {
            
            @Override
            public Optional<String> generateUid(String name) {
                return Optional.of(name + "-ExampleWorkflow");
            }
        };
        
        DataStream<EnrichedRecord> result = dsa.produceDataStream(pc, env)
                .map(new ConvertToEnrichedRecord(config))
                .name("Convert RowData to EnrichedRecord");

        return result;
    }

    private File getTestDir(String subdir, boolean clean) throws IOException {
        String path = String.format("target/test/%s", this.getClass().getSimpleName());
        File classDir = new File(path);
        File testDir = new File(classDir.getAbsoluteFile(), subdir);
        
        if (clean) {
            FileUtils.deleteDirectory(testDir);
        }
        
        return testDir;
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
