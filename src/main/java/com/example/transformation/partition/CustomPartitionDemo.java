package com.example.transformation.partition;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/***
 * @author: BYDylan
 * @date: 2024-09-04 23:21:36
 * @description: 自定义分区
 */
public class CustomPartitionDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.setParallelism(2);
        DataStreamSource<String> socketDS = env.socketTextStream("hadoop102", 7777);
        socketDS.partitionCustom(new CustomPartitioner(), r -> r).print();
        env.execute();
    }
}
