package com.scaleunlimited;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.commons.io.FileUtils;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.io.InputFormat;
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
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.table.HoodieTableSource;
import org.junit.jupiter.api.Test;

class ExampleWorkflowTest {

    private static final int PARALLELISM = 2;
    private static final int MAX_PARALLELISM = 8192;
    private static final long CHECKPOINT_INTERVAL_MS = 10 * 1000L;

    private static final int NUM_RESULTS = 100_000;
    
    @Test
    void testHudiIncrementalQuery() throws Exception {
        File testDir = getTestDir("testHudiIncrementalQuery", true);
        File inputDir = writeHudiFiles(testDir);
        
        Configuration conf = new Configuration();
        StreamExecutionEnvironment env = makeExecutionEnvironment(testDir, conf);

        new ExampleReaderWorkflow()
            .setInput(makeHudiInput(env, conf, inputDir.getAbsolutePath()))
            .build();
    
        JobClient readerClient = env.executeAsync("reader workflow");
        
        while (!isDone(readerClient.getJobStatus().get())) {
            Thread.sleep(1000);
        }
        
        Integer finalReadCount = (Integer)readerClient.getJobExecutionResult().get().getAccumulatorResult(ExampleReaderWorkflow.RECORD_COUNTER_NAME);
        assertEquals(NUM_RESULTS, (int)finalReadCount);
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
        
        JobClient writerClient = env.executeAsync("writer workflow");
        
        // Wait for table to be created, before starting up reader
        while (!isDone(writerClient.getJobStatus().get()) && !tableExists(outputDir)) {
            Thread.sleep(10);
        }
        
        StreamExecutionEnvironment env2 = makeExecutionEnvironment(testDir, conf);
        
        new ExampleReaderWorkflow()
            .setInput(makeHudiInput(env2, conf, outputDir.getAbsolutePath()))
            .build();
        
        JobClient readerClient = env2.executeAsync("reader workflow");
        
        JobStatus writerStatus = null;
        JobStatus readerStatus = null;
        
        while (!isDone(writerStatus) && !isDone(readerStatus)) {
            writerStatus = writerClient.getJobStatus().get();
            System.out.println("Writer: " + writerStatus);

            readerStatus = readerClient.getJobStatus().get();
            System.out.println("Reader: " + readerStatus);

            Thread.sleep(1000);
        }
        
        Integer finalWriteCount = (Integer)writerClient.getJobExecutionResult().get().getAccumulatorResult(ExampleWriterWorkflow.RECORD_COUNTER_NAME);
        assertEquals(NUM_RESULTS, (int)finalWriteCount);
        
        Integer finalReadCount = (Integer)readerClient.getJobExecutionResult().get().getAccumulatorResult(ExampleReaderWorkflow.RECORD_COUNTER_NAME);
        assertEquals(NUM_RESULTS, (int)finalReadCount);
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

    private boolean tableExists(File outputDir) {
        if (!outputDir.exists()) {
            return false;
        }
        
        File[] partitions = outputDir.listFiles(new FilenameFilter() {
            
            @Override
            public boolean accept(File dir, String name) {
                // Partition dir looks like 20230112-1706
                return Pattern.matches("\\d{8}\\-\\d{4}", name);
            }
        });
        
        if (partitions.length == 0) {
            return false;
        }
        
        File partitionDir = partitions[0];
        if (!partitionDir.isDirectory()) {
            return false;
        }
        
        File[] parquetFiles = partitionDir.listFiles(new FilenameFilter() {
            
            @Override
            public boolean accept(File dir, String name) {
                return name.endsWith(".parquet");
            }
        });
        
        return parquetFiles.length > 0;
    }

    private boolean isDone(JobStatus status) {
        return (status == JobStatus.CANCELED)
                || (status == JobStatus.FAILED)
                || (status == JobStatus.FINISHED);
    }

    private DataStream<EnrichedRecord> makeHudiInput(StreamExecutionEnvironment env,
            Configuration config, String tableDir) {
        HudiUtils.setHudiWriteConfig(config, tableDir, PARALLELISM);
        config.set(FlinkOptions.QUERY_TYPE, FlinkOptions.QUERY_TYPE_INCREMENTAL);
        config.setBoolean(FlinkOptions.READ_AS_STREAMING, true);
        config.set(FlinkOptions.READ_TASKS, PARALLELISM);
        // TODO - I assume commit date is stored in state, for recovery
        config.set(FlinkOptions.READ_START_COMMIT, FlinkOptions.START_COMMIT_EARLIEST);

        DataType rowDataType = org.apache.hudi.util.AvroSchemaConverter.convertToDataType(EnrichedRecord.SCHEMA$);
        String[] partitionKeys = new String[] {HudiConstants.PARTITION_PATH_FIELD};
        HoodieTableSource hoodieTableSource =
                new HoodieTableSource(
                        makeResolvedSchema(rowDataType),
                        new org.apache.hadoop.fs.Path(tableDir),
                        Arrays.asList(partitionKeys),
                        "no_partition",
                        config);

        InputFormat<RowData, ?> inputFormat = hoodieTableSource.getInputFormat();

        DataStream<EnrichedRecord> result =
                env.createInput(inputFormat, TypeInformation.of(RowData.class))
                        .map(new ConvertToEnrichedRecord(config));

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
