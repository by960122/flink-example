package com.example.transformation.combine;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/***
 * @author: BYDylan
 * @date: 2024-09-21 10:30:00
 * @description: 合流: 流的数据类型必须一致, 一次可以合并多条流
 */
public class UnionDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Integer> source1 = env.fromElements(1, 2, 3);
        DataStreamSource<Integer> source2 = env.fromElements(11, 22, 33);
        DataStreamSource<String> source3 = env.fromElements("111", "222", "333");

        DataStream<Integer> union = source1.union(source2).union(source3.map(Integer::valueOf));
        union.print();
        env.execute();
    }
}
