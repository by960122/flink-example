package com.example.transformation.partition;

import org.apache.flink.api.common.functions.Partitioner;

public class CustomPartitioner implements Partitioner<String> {
    @Override
    public int partition(String key, int numPartitions) {
        return Integer.parseInt(key) % numPartitions;
    }
}
