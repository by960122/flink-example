package com.example.transformation.combine;

import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

/***
 * @author: BYDylan
 * @date: 2024-09-21 10:31:36
 * @description: 连接: 流的联合虽然简单, 不过受限于数据类型不能改变 1. 一次只能连接两条流 2. 流的数据类型可以不一样 3. 连接后可以调用 map、flatmap、process来处理, 但是各处理各的
 */
public class ConnectDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // DataStreamSource<Integer> source1 = env.fromElements(1, 2, 3);
        // DataStreamSource<String> source2 = env.fromElements("a", "b", "c");

        SingleOutputStreamOperator<Integer> source1 = env.socketTextStream("hadoop102", 7777).map(Integer::parseInt);
        DataStreamSource<String> source2 = env.socketTextStream("hadoop102", 8888);
        ConnectedStreams<Integer, String> connect = source1.connect(source2);
        SingleOutputStreamOperator<String> result = connect.map(new CoMapFunction<>() {
            @Override
            public String map1(Integer value) {
                return "来源于数字流:" + value.toString();
            }
            @Override
            public String map2(String value) {
                return "来源于字母流:" + value;
            }
        });
        result.print();
        env.execute();
    }
}
