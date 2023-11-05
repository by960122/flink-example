package com.example.source;

import java.util.Arrays;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/***
 * @author: BYDylan
 * @date: 2024-09-01 22:51:23
 * @description: 从集合中读取数据
 */
public class CollectionSourceDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        // 从元素读
        // DataStreamSource<Integer> source = env.fromElements(1, 2, 33);
        // 从集合读
        DataStreamSource<Integer> source = env.fromCollection(Arrays.asList(1, 22, 3));
        source.print();
        env.execute("collection source");
    }
}
