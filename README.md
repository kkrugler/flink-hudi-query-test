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

