package com.example.window;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import com.example.functions.WaterSensorProcessWindowFunction;
import com.example.model.WaterSensor;

/***
 * @author: BYDylan
 * @date: 2024-09-21 16:00:08
 * @description: 全窗口函数: 有些场景下, 我们要做的计算必须基于全部的数据才有效, 这时做增量聚合就没什么意义了; 另外, 输出的结果有可能要包含上下文中的一些信息（比如窗口的起始时间）, 这是增量聚合函数做不到的
 *               窗口函数(WindowFunction): 能提供的上下文信息较少, 处理窗口函数(ProcessWindowFunction): 是 Window API 中最底层的通用窗口函数接口.
 *               之所以说它“最底层”, 是因为除了可以拿到窗口中的所有数据之外, ProcessWindowFunction 还可以获取到一个“上下文对象”（Context）. 这个上下文对象非常强大,
 *               不仅能够获取窗口信息, 还可以访问当前的时间和状态信息. 这里的时间就包括了处理时间（processing time）和事件时间水位线（event time watermark）. 这就使得
 *               ProcessWindowFunction 更加灵活、功能更加丰富, 其实就是一个增强版的 WindowFunction
 */
public class FullProcessWindowDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<WaterSensor> sensorDS =
            env.fromElements(new WaterSensor("s1", 1L, 1), new WaterSensor("s2", 2L, 2), new WaterSensor("s3", 3L, 3));
        KeyedStream<WaterSensor, String> sensorKS = sensorDS.keyBy(WaterSensor::getId);
        // 1. 窗口分配器
        WindowedStream<WaterSensor, String, TimeWindow> sensorWS =
            sensorKS.window(TumblingProcessingTimeWindows.of(Time.seconds(10)));
        // WindowFunction
        // sensorWS.apply(new WaterSensorWindowFunction());
        SingleOutputStreamOperator<String> process = sensorWS.process(new WaterSensorProcessWindowFunction());
        process.print();
        env.execute();
    }
}
