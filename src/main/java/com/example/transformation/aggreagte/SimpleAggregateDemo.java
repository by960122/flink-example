package com.example.transformation.aggreagte;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import com.example.model.WaterSensor;

/***
 * @author: BYDylan
 * @date: 2024-09-21 10:54:11
 * @description: 聚合算子
 */
public class SimpleAggregateDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<WaterSensor> sensorDS = env.fromElements(
                new WaterSensor("s1", 1L, 1),
                new WaterSensor("s1", 11L, 11),
                new WaterSensor("s2", 2L, 2),
                new WaterSensor("s3", 3L, 3)
        );
        KeyedStream<WaterSensor, String> sensorKS =
            sensorDS.keyBy((KeySelector<WaterSensor, String>)WaterSensor::getId);
        // 简单聚合算子, keyby之后才能调用
        // 传位置索引的, 适用于 Tuple类型, POJO不行
        // SingleOutputStreamOperator<WaterSensor> result = sensorKS.sum(2);
        // SingleOutputStreamOperator<WaterSensor> result = sensorKS.sum("vc");
        // SingleOutputStreamOperator<WaterSensor> result = sensorKS.max("vc");
        // SingleOutputStreamOperator<WaterSensor> result = sensorKS.min("vc");
        SingleOutputStreamOperator<WaterSensor> result = sensorKS.maxBy("vc");
        // SingleOutputStreamOperator<WaterSensor> result = sensorKS.minby("vc");
        result.print();
        env.execute();
    }
}
