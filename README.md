Some issues I've run across...

## Incremental Query

The `testHudiWriteAndIncrementalRead` test fails, because nothing is read from the Hudi table unless I put a
delay in between when the write workflow starts running, and when I start the read workflow. The delay
has to be long enough for at least one checkpoint has happened with the write workflow.

This is an improvement over the previous situation, where no matter what I did, I couldn't read any data.

## Javalin

1. Why does Javalin need to be running when writing to Hudi?
1. When running on a Flink cluster, Javalin fails to start due to an error creating the `WebSocketServerFactory`. I think this could be due to class conflicts, where the Flink infrastructure also uses Jetty. It feels like Hudi should shade any networking code it uses, to avoid this type of problem.
1. Related is that when Javalin can't start up properly, it automatically stops itself. But then the Hudi code that's trying to start up Javalin fails, because once Javalin has been stopped, you need to create a new instance of the app if you want to try to start it again. It's this code:
``` java
    for (int attempt = 0; attempt < START_SERVICE_MAX_RETRIES; attempt++) {
      // Returns port to try when trying to bind a service. Handles wrapping and skipping privileged ports.
      int tryPort = port == 0 ? port : (port + attempt - 1024) % (65536 - 1024) + 1024;
      try {
        app.start(tryPort);
        return app.port();
      } catch (Exception e) {
        if (e.getMessage() != null && e.getMessage().contains("Failed to bind to")) {
          if (tryPort == 0) {
            LOG.warn("Timeline server could not bind on a random free port.");
          } else {
            LOG.warn(String.format("Timeline server could not bind on port %d. "
                + "Attempting port %d + 1.",tryPort, tryPort));
          }
        } else {
          LOG.warn(String.format("Timeline server start failed on port %d. Attempting port %d + 1.",tryPort, tryPort), e);
        }
      }
    }
    throw new IOException(String.format("Timeline server start failed on port %d, after retry %d times", port, START_SERVICE_MAX_RETRIES));
```

## Embedded Timeline Server disabled

I see this logged while running:

```
1142 [Enrich events -> Map -> hoodie_bulk_insert_write: example-table -> Sink: dummy (2/2)#0] INFO  org.apache.hudi.client.BaseHoodieClient  - Embedded Timeline Server is disabled. Not starting timeline service
```

But I thought Hudi needed a timeline service when writing?

## Logging

Log4J is used in 386 source files, via counting occurrences of `import org.apache.log4j.LogManager`.

Slf4J is used in 79 source files, via counting occurrences of `import org.slf4j.LoggerFactory`.

Then there's also the use of `com.esotericsoftware.minlog.Log`, e.g. in `WriteMarkersFactory.java`:

``` java
          Log.warn("Timeline-server-based markers are configured as the marker type "
              + "but embedded timeline server is not enabled.  Falling back to direct markers.");
```

This (by default) is writing to `System.out`.

This makes it very hard for developers to know that all of the logging output is being captured.

Shouldn't everything use Slf4J, and then the client can configure the appropriate logging infrastructure?

## No schema found for reader if table doesn't exist

If I start the reader code before the Hudi table has been sufficiently started, I see this:

```
0    [flink-akka.actor.default-dispatcher-8] INFO  org.apache.hudi.common.table.HoodieTableMetaClient  - Initializing /Users/kenkrugler/git/flink-hudi-query-test/target/test/ExampleWorkflowTest/testHudiIncrementalQuery/hudi-table as hoodie table /Users/kenkrugler/git/flink-hudi-query-test/target/test/ExampleWorkflowTest/testHudiIncrementalQuery/hudi-table
23/01/09 15:29:01 WARN table.HoodieTableSource:473 - Get table avro schema error, use schema from the DDL instead
java.lang.NullPointerException: null
    at org.apache.hudi.common.table.TableSchemaResolver.getTableAvroSchema(TableSchemaResolver.java:127) ~[hudi-common-0.12.0.jar:0.12.0]
    at org.apache.hudi.table.HoodieTableSource.getTableAvroSchema(HoodieTableSource.java:470) ~[hudi-flink-0.12.0.jar:0.12.0]
    at org.apache.hudi.table.HoodieTableSource.getBatchInputFormat(HoodieTableSource.java:335) ~[hudi-flink-0.12.0.jar:0.12.0]
    at org.apache.hudi.table.HoodieTableSource.getInputFormat(HoodieTableSource.java:331) ~[hudi-flink-0.12.0.jar:0.12.0]
    at org.apache.hudi.table.HoodieTableSource.getInputFormat(HoodieTableSource.java:326) ~[hudi-flink-0.12.0.jar:0.12.0]
    at com.scaleunlimited.ExampleWorkflowTest.makeHudiInput(ExampleWorkflowTest.java:149) ~[test-classes/:?]
    at com.scaleunlimited.ExampleWorkflowTest.testHudiIncrementalQuery(ExampleWorkflowTest.java:94) ~[test-classes/:?]
```

Shouldn't the Hudi reader code be able to get the Avro schema from the configuration, the same as the writer?

## Classloader bug

If I run the workflow twice in the same Task Manager, I get this exception:

```
2022-12-16 14:01:15
java.io.IOException: java.io.IOException: Exception happened when bulk insert.
    at org.apache.hudi.sink.bulk.BulkInsertWriterHelper.write(BulkInsertWriterHelper.java:118)
    at org.apache.hudi.sink.bulk.BulkInsertWriteFunction.processElement(BulkInsertWriteFunction.java:124)
    at org.apache.flink.streaming.api.operators.ProcessOperator.processElement(ProcessOperator.java:66)
    at org.apache.flink.streaming.runtime.tasks.ChainingOutput.pushToOperator(ChainingOutput.java:99)
    at org.apache.flink.streaming.runtime.tasks.ChainingOutput.collect(ChainingOutput.java:80)
    at org.apache.flink.streaming.runtime.tasks.ChainingOutput.collect(ChainingOutput.java:39)
    at org.apache.flink.streaming.api.operators.CountingOutput.collect(CountingOutput.java:56)
    at org.apache.flink.streaming.api.operators.CountingOutput.collect(CountingOutput.java:29)
    at org.apache.flink.streaming.api.operators.StreamMap.processElement(StreamMap.java:38)
    at org.apache.flink.streaming.runtime.tasks.ChainingOutput.pushToOperator(ChainingOutput.java:99)
    at org.apache.flink.streaming.runtime.tasks.ChainingOutput.collect(ChainingOutput.java:80)
    at org.apache.flink.streaming.runtime.tasks.ChainingOutput.collect(ChainingOutput.java:39)
    at org.apache.flink.streaming.api.operators.CountingOutput.collect(CountingOutput.java:56)
    at org.apache.flink.streaming.api.operators.CountingOutput.collect(CountingOutput.java:29)
    at org.apache.flink.streaming.api.operators.StreamFilter.processElement(StreamFilter.java:39)
    at org.apache.flink.streaming.runtime.tasks.ChainingOutput.pushToOperator(ChainingOutput.java:99)
    at org.apache.flink.streaming.runtime.tasks.ChainingOutput.collect(ChainingOutput.java:80)
    at org.apache.flink.streaming.runtime.tasks.ChainingOutput.collect(ChainingOutput.java:39)
    at org.apache.flink.streaming.api.operators.CountingOutput.collect(CountingOutput.java:56)
    at org.apache.flink.streaming.api.operators.CountingOutput.collect(CountingOutput.java:29)
    at org.apache.flink.streaming.api.operators.TimestampedCollector.collect(TimestampedCollector.java:51)
    at com.bloomberg.idp.enrichment.functions.AddIpBasedEnrichments.processElement(AddIpBasedEnrichments.java:248)
    at com.bloomberg.idp.enrichment.functions.AddIpBasedEnrichments.processElement(AddIpBasedEnrichments.java:42)
    at org.apache.flink.streaming.api.operators.co.CoBroadcastWithNonKeyedOperator.processElement1(CoBroadcastWithNonKeyedOperator.java:110)
    at org.apache.flink.streaming.runtime.io.StreamTwoInputProcessorFactory.processRecord1(StreamTwoInputProcessorFactory.java:217)
    at org.apache.flink.streaming.runtime.io.StreamTwoInputProcessorFactory.lambda$create$0(StreamTwoInputProcessorFactory.java:183)
    at org.apache.flink.streaming.runtime.io.StreamTwoInputProcessorFactory$StreamTaskNetworkOutput.emitRecord(StreamTwoInputProcessorFactory.java:266)
    at org.apache.flink.streaming.runtime.io.AbstractStreamTaskNetworkInput.processElement(AbstractStreamTaskNetworkInput.java:134)
    at org.apache.flink.streaming.runtime.io.AbstractStreamTaskNetworkInput.emitNext(AbstractStreamTaskNetworkInput.java:105)
    at org.apache.flink.streaming.runtime.io.StreamOneInputProcessor.processInput(StreamOneInputProcessor.java:65)
    at org.apache.flink.streaming.runtime.io.StreamMultipleInputProcessor.processInput(StreamMultipleInputProcessor.java:85)
    at org.apache.flink.streaming.runtime.tasks.StreamTask.processInput(StreamTask.java:519)
    at org.apache.flink.streaming.runtime.tasks.mailbox.MailboxProcessor.runMailboxLoop(MailboxProcessor.java:203)
    at org.apache.flink.streaming.runtime.tasks.StreamTask.runMailboxLoop(StreamTask.java:804)
    at org.apache.flink.streaming.runtime.tasks.StreamTask.invoke(StreamTask.java:753)
    at org.apache.flink.runtime.taskmanager.Task.runWithSystemExitMonitoring(Task.java:948)
    at org.apache.flink.runtime.taskmanager.Task.restoreAndInvoke(Task.java:927)
    at org.apache.flink.runtime.taskmanager.Task.doRun(Task.java:741)
    at org.apache.flink.runtime.taskmanager.Task.run(Task.java:563)
    at java.base/java.lang.Thread.run(Thread.java:834)
Caused by: java.io.IOException: Exception happened when bulk insert.
    at org.apache.hudi.sink.bulk.BulkInsertWriterHelper.write(BulkInsertWriterHelper.java:116)
    ... 39 more
Caused by: java.lang.ClassCastException: class org.apache.hudi.common.fs.HoodieWrapperFileSystem cannot be cast to class org.apache.hudi.common.fs.HoodieWrapperFileSystem (org.apache.hudi.common.fs.HoodieWrapperFileSystem is in unnamed module of loader org.apache.flink.util.ChildFirstClassLoader @22441eda; org.apache.hudi.common.fs.HoodieWrapperFileSystem is in unnamed module of loader org.apache.flink.util.ChildFirstClassLoader @c949447)
    at org.apache.hudi.io.storage.row.HoodieRowDataParquetWriter.<init>(HoodieRowDataParquetWriter.java:51)
    at org.apache.hudi.io.storage.row.HoodieRowDataFileWriterFactory.newParquetInternalRowFileWriter(HoodieRowDataFileWriterFactory.java:79)
    at org.apache.hudi.io.storage.row.HoodieRowDataFileWriterFactory.getRowDataFileWriter(HoodieRowDataFileWriterFactory.java:55)
    at org.apache.hudi.io.storage.row.HoodieRowDataCreateHandle.createNewFileWriter(HoodieRowDataCreateHandle.java:211)
    at org.apache.hudi.io.storage.row.HoodieRowDataCreateHandle.<init>(HoodieRowDataCreateHandle.java:103)
    at org.apache.hudi.sink.bulk.BulkInsertWriterHelper.getRowCreateHandle(BulkInsertWriterHelper.java:133)
    at org.apache.hudi.sink.bulk.BulkInsertWriterHelper.write(BulkInsertWriterHelper.java:111)
    ... 39 more
```

I'm wondering if there's some code in Hudi that's hanging onto a classloader (in a thread?), and that's why we have the same Hudi class being found in two different instances of Flink's `ChildFirstClassLoader`.
 