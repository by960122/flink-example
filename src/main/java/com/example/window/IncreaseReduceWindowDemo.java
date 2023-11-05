package com.example.window;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import com.example.functions.WaterSensorReduceFunction;
import com.example.model.WaterSensor;

import lombok.extern.slf4j.Slf4j;

/***
 * @author: BYDylan
 * @date: 2024-09-21 15:44:18
 * @description: 窗口函数-增量聚合函数-归约函数(ReduceFunction) 这个接口有一个限制, 就是聚合状态的类型, 输出结果的类型都必须和输入数据类型一样
 */
@Slf4j
public class IncreaseReduceWindowDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<WaterSensor> sensorDS =
            env.fromElements(new WaterSensor("s1", 1L, 1), new WaterSensor("s2", 2L, 2), new WaterSensor("s3", 3L, 3));
        KeyedStream<WaterSensor, String> sensorKS = sensorDS.keyBy(WaterSensor::getId);
        // 1. 窗口分配器
        WindowedStream<WaterSensor, String, TimeWindow> sensorWS = sensorKS.window(TumblingProcessingTimeWindows.of(Time.seconds(10)));
        // 2. 窗口函数：增量聚合 Reduce
        // 窗口的reduce：
        // 1、相同key的第一条数据来的时候, 不会调用reduce方法
        // 2、增量聚合：后续来一条数据, 就会计算一次, 但是不会输出
        // 3、在窗口触发的时候, 才会输出窗口的最终计算结果
        SingleOutputStreamOperator<WaterSensor> reduce = sensorWS.reduce(new WaterSensorReduceFunction());
        reduce.print();
        env.execute();
    }
}
