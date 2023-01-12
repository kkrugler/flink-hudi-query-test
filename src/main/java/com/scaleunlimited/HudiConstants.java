package com.scaleunlimited;

public class HudiConstants {

    public static final String TABLE_NAME = "example-table";
    
    // The field in our output record that is the unique key
    // TODO - do we need to set this, with bulk insert?
    public static final String RECORD_KEY_FIELD = "key";
    
    // The field in our output record that contains the string used
    // to decide which partition (sub-directory) to write into.
    public static final String PARTITION_PATH_FIELD = "partition";

    // Per-minute output directories.
    public static final String PARTITION_OUTPUT_FORMAT = "yyyyMMdd-HHmm";
    
}
