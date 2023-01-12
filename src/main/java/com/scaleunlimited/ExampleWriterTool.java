package com.scaleunlimited;

import java.util.List;
import java.util.concurrent.Callable;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import picocli.CommandLine;
import picocli.CommandLine.Option;

public class ExampleWriterTool implements Callable<Integer> {

    @Option(
            names = "--num-records",
            description = "number of example records to generate")
    int numRecords = 1_000_000;

    @Option(
            names = "--output-dir",
            description = "Directory for Hudi table",
            required = true)
    String outputDir;

    @Option(
            names = "--parallelism",
            description = "Parallelism for workflow")
    int parallelism = 2;

    @Option(
            names = "--checkpoint-interval",
            description = "How often to take checkpoints (in milliseconds)")
    long checkpointInterval = 10 * 1000L;

    public static void main(String[] args) {
        int exitCode = new CommandLine(new ExampleWriterTool()).execute(args);
        if (exitCode != 0) {
            System.exit(exitCode);
        }
    }
    
    @Override
    public Integer call() throws Exception {
        Configuration conf = new Configuration();
        
        // Set up for final checkpoint (and thus final commit of inflight data) when workflow ends
        conf.setBoolean(
                ExecutionCheckpointingOptions.ENABLE_CHECKPOINTS_AFTER_TASKS_FINISH, true);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(parallelism);
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        env.enableCheckpointing(checkpointInterval, CheckpointingMode.EXACTLY_ONCE);
        env.setStateBackend(new HashMapStateBackend());
        env.getConfig().enableObjectReuse();
        
        DataStream<ExampleRecord> input = env
                .addSource(new ExampleSource(numRecords), "Example Source")
                .assignTimestampsAndWatermarks(WatermarkStrategy.noWatermarks())
                .setParallelism(parallelism);
        
        List<EnrichmentRecord> enrichments = EnrichmentSource.makeEnrichments();

        new ExampleWriterWorkflow()
            .setInput(input)
            .setEnrichments(env.addSource(new EnrichmentSource(enrichments)))
            .setOutput(HudiUtils.makeHudiOutput(env, conf, outputDir, parallelism))
            .build();

        env.execute("writer workflow");
        
        return 0;
    }
}
