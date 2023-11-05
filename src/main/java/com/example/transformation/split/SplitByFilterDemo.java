package com.example.transformation.split;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/***
 * @author: BYDylan
 * @date: 2024-09-04 23:28:09
 * @description: 分流: 奇数、偶数拆分成不同流
 */
public class SplitByFilterDemo {
    public static void main(String[] args) throws Exception {
        // StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        env.setParallelism(1);
        DataStreamSource<String> socketDS = env.socketTextStream("hadoop102", 7777);

        SingleOutputStreamOperator<String> even = socketDS.filter(value -> Integer.parseInt(value) % 2 == 0);
        SingleOutputStreamOperator<String> odd = socketDS.filter(value -> Integer.parseInt(value) % 2 == 1);
        even.print("偶数流");
        odd.print("奇数流");
        env.execute();
    }
}