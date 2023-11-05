package com.example.window;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import com.example.functions.WaterSensorAggregateFunction;
import com.example.functions.WaterSensorProcessWindowFunction;
import com.example.model.WaterSensor;

/***
 * @author: BYDylan
 * @date: 2024-09-21 16:11:09
 * @description: 增量聚合和全窗口函数的结合使用 增量聚合 Aggregate + 全窗口 process 1、增量聚合函数处理数据： 来一条计算一条 2、窗口触发时， 增量聚合的结果（只有一条） 传递给 全窗口函数
 *               3、经过全窗口函数的处理包装后，输出
 *
 *               结合两者的优点： 1、增量聚合： 来一条计算一条，存储中间的计算结果，占用的空间少 2、全窗口函数： 可以通过 上下文 实现灵活的功能
 */
public class AggregateAndProcessWindowDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<WaterSensor> sensorDS =
            env.fromElements(new WaterSensor("s1", 1L, 1), new WaterSensor("s2", 2L, 2), new WaterSensor("s3", 3L, 3));
        KeyedStream<WaterSensor, String> sensorKS = sensorDS.keyBy(WaterSensor::getId);
        // 1. 窗口分配器
        WindowedStream<WaterSensor, String, TimeWindow> sensorWS =
            sensorKS.window(TumblingProcessingTimeWindows.of(Time.seconds(10)));
        // 2. 窗口函数：
        // sensorWS.reduce(new WaterSensorReduceFunction(), new WaterSensorWindowFunction());
        // sensorWS.reduce(new WaterSensorReduceFunction(), new WaterSensorProcessWindowFunction());
        // SingleOutputStreamOperator<String> result = sensorWS.aggregate(new WaterSensorAggregateFunction(), new
        // WaterSensorWindowFunction());
        SingleOutputStreamOperator<String> result =
            sensorWS.aggregate(new WaterSensorAggregateFunction(), new WaterSensorProcessWindowFunction());
        result.print();
        env.execute();
    }
}
