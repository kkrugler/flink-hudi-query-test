package com.scaleunlimited;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;

import org.apache.commons.io.FileUtils;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.hudi.configuration.FlinkOptions;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.scaleunlimited.functions.CountRecordsReadFunction;
import com.scaleunlimited.records.EnrichmentRecord;
import com.scaleunlimited.records.RawRecord;

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
            .setInput(HudiUtils.makeHudiInput(env, conf, inputDir.getAbsolutePath(), PARALLELISM))
            .build();
    
        CountRecordsReadFunction.resetCount();
        JobClient readerClient = env.executeAsync("reader workflow");
        
        int readCount = 0;
        // Set max time based on number of records, and at least one checkpoint
        long maxTime = System.currentTimeMillis() + Math.max(CHECKPOINT_INTERVAL_MS * 2, NUM_RESULTS / 10);
        while ((readCount < NUM_RESULTS) && (System.currentTimeMillis() < maxTime)) {
            Thread.sleep(100);
            readCount = CountRecordsReadFunction.getCount();
        }
        
        readerClient.cancel();
        
        assertEquals(NUM_RESULTS, readCount);
    }
    
    private File writeHudiFiles(File testDir) throws Exception {
        Configuration conf = new Configuration();
        StreamExecutionEnvironment env = makeExecutionEnvironment(testDir, conf);
        
        DataStream<RawRecord> input = env
                .addSource(new RawSource(NUM_RESULTS), "Example Source")
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
        
        DataStream<RawRecord> input = env
                .addSource(new RawSource(NUM_RESULTS), "Example Source")
                .assignTimestampsAndWatermarks(WatermarkStrategy.noWatermarks())
                .setParallelism(PARALLELISM);
        
        List<EnrichmentRecord> enrichments = EnrichmentSource.makeEnrichments();

        new ExampleWriterWorkflow()
            .setInput(input)
            .setEnrichments(env.addSource(new EnrichmentSource(enrichments)))
            .setOutput(HudiUtils.makeHudiOutput(env, conf, outputDir.getAbsolutePath(), PARALLELISM))
            .build();
        
        LOGGER.info("Starting execution of writer workflow...");
        JobClient writerClient = env.executeAsync("writer workflow");
        
        // Wait for table to be created, before starting up reader
        // If we don't do this, the reader says that no table exists
        LOGGER.info("Waiting for Hudi table to exist...");
        Thread.sleep(CHECKPOINT_INTERVAL_MS * 2);
        
        StreamExecutionEnvironment env2 = makeExecutionEnvironment(testDir, conf);
        
        // Check every 10 seconds, to speed up test
        conf.set(FlinkOptions.READ_STREAMING_CHECK_INTERVAL, 10);
        new ExampleReaderWorkflow()
            .setInput(HudiUtils.makeHudiInput(env2, conf, outputDir.getAbsolutePath(), PARALLELISM))
            .build();
        
        LOGGER.info("Starting execution of reader workflow...");
        CountRecordsReadFunction.resetCount();
        JobClient readerClient = env2.executeAsync("reader workflow");
        
//        // Wait for write workflow to finish.
//        LOGGER.info("Waiting for writer workflow to finish...");
//        long maxTime = System.currentTimeMillis() + NUM_RESULTS;
//        while ((writerClient.getJobStatus().get() == JobStatus.RUNNING) && (System.currentTimeMillis() < maxTime)) {
//            Thread.sleep(100);
//        }
//        
//        assertNotEquals(JobStatus.RUNNING, writerClient.getJobStatus().get());
        
        LOGGER.info("Waiting for reader workflow to start...");
        long maxTime = System.currentTimeMillis() + 5_000L;
        while ((readerClient.getJobStatus().get() != JobStatus.RUNNING) && (System.currentTimeMillis() < maxTime)) {
            Thread.sleep(100);
        }
        assertEquals(JobStatus.RUNNING, readerClient.getJobStatus().get());

        LOGGER.info("Waiting for writer & reader workflows to finish...");
        int readCount = 0;
        // Set max time based on number of records, and at least one checkpoint, plus 2ms/result
        // TODO - Hudi reader doesn't seem to be picking up new records for a really long time,
        // 
        maxTime = System.currentTimeMillis() + Math.max(CHECKPOINT_INTERVAL_MS * 2, NUM_RESULTS * 2);
        while ((readCount < NUM_RESULTS)
            && (!isDone(writerClient) || (System.currentTimeMillis() < maxTime))) {
            Thread.sleep(100);
            readCount = CountRecordsReadFunction.getCount();
        }
        
        readerClient.cancel();
        
        assertEquals(NUM_RESULTS, readCount);
    }

    private boolean isDone(JobClient client) throws InterruptedException, ExecutionException {
        switch (client.getJobStatus().get()) {
            case FINISHED:
            case SUSPENDED:
            case FAILED:
            case CANCELED:
                return true;
                
                default:
                    return false;
        }
    }

    private StreamExecutionEnvironment makeExecutionEnvironment(File testDir, Configuration conf) throws Exception {
        File logFile = new File(testDir, "flink-logs");
        logFile.mkdirs();
        System.setProperty("log.file", logFile.getAbsolutePath());

        // Set up for final checkpoint (and thus final commit of in-flight data) when workflow ends
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

    private File getTestDir(String subdir, boolean clean) throws IOException {
        String path = String.format("target/test/%s", this.getClass().getSimpleName());
        File classDir = new File(path);
        File testDir = new File(classDir.getAbsoluteFile(), subdir);
        
        if (clean) {
            FileUtils.deleteDirectory(testDir);
        }
        
        return testDir;
    }

}
