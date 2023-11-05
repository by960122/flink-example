package com.example.transformation;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import com.example.model.WaterSensor;

/***
 * @author: BYDylan
 * @date: 2024-09-01 23:37:14
 * @description: 如果输入的数据是s1, 只打印vc; 如果输入的数据是s2, 既打印ts又打印vc
 */
public class FlatmapDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<WaterSensor> sensorDS = env.fromElements(new WaterSensor("s1", 1L, 1),
            new WaterSensor("s1", 11L, 11), new WaterSensor("s2", 2L, 2), new WaterSensor("s3", 3L, 3));

        SingleOutputStreamOperator<String> flatmap =
            sensorDS.flatMap((FlatMapFunction<WaterSensor, String>)(value, out) -> {
                if ("s1".equals(value.getId())) {
                    // 如果是 s1, 输出 vc
                    out.collect(value.getVc().toString());
                } else if ("s2".equals(value.getId())) {
                    // 如果是 s2, 分别输出ts和vc
                    out.collect(value.getTs().toString());
                    out.collect(value.getVc().toString());
                }
            });

        flatmap.print();
        env.execute();
    }
}
