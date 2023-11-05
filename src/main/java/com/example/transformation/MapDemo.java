package com.example.transformation;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import com.example.functions.WaterSensorIdMapFunction;
import com.example.model.WaterSensor;

/***
 * @author: BYDylan
 * @date: 2024-09-01 23:26:45
 * @description: 转换算子: map
 */
public class MapDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<WaterSensor> sensorDS = env.fromElements(
                new WaterSensor("s1", 1L, 1),
                new WaterSensor("s2", 2L, 2),
                new WaterSensor("s3", 3L, 3)
        );
        // 方式一: 匿名实现类
        // SingleOutputStreamOperator<String> map = sensorDS.map((MapFunction<WaterSensor, String>) WaterSensor::getId);
        // 方式二: lambda表达式
        // SingleOutputStreamOperator<String> map = sensorDS.map(WaterSensor::getId);
        // 方式三: 定义一个类来实现MapFunction
        SingleOutputStreamOperator<String> map = sensorDS.map(new WaterSensorIdMapFunction());
        map.print();
        env.execute();
    }
}
