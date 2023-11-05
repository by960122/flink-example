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
import com.example.model.WaterSensor;

import lombok.extern.slf4j.Slf4j;

/***
 * @author: BYDylan
 * @date: 2024-09-21 15:48:11
 * @description: 窗口函数-增量聚合函数-聚合函数(AggregateFunction) ReduceFunction 可以解决大多数归约聚合的问题, 但是这个接口有一个限制,
 *               就是聚合状态的类型、输出结果的类型都必须和输入数据类型一样。 aggregate 就突破了这个限制, 可以定义更加灵活的窗口聚合操作. 这个方法需要传入一个 AggregateFunction
 *               的实现类作为参数。
 */
@Slf4j
public class IncreaseAggregateWindowDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<WaterSensor> sensorDS =
            env.fromElements(new WaterSensor("s1", 1L, 1), new WaterSensor("s2", 2L, 2), new WaterSensor("s3", 3L, 3));
        KeyedStream<WaterSensor, String> sensorKS = sensorDS.keyBy(WaterSensor::getId);
        // 窗口分配器
        WindowedStream<WaterSensor, String, TimeWindow> sensorWS = sensorKS.window(TumblingProcessingTimeWindows.of(Time.seconds(10)));
        // 窗口函数： 增量聚合 Aggregate
        // 1、属于本窗口的第一条数据来, 创建窗口, 创建累加器
        // 2、增量聚合： 来一条计算一条, 调用一次add方法
        // 3、窗口输出时调用一次getresult方法
        // 4、输入、中间累加器、输出 类型可以不一样, 非常灵活
        SingleOutputStreamOperator<WaterSensor> aggregate = sensorWS.aggregate(new WaterSensorAggregateFunction());
        aggregate.print();
        env.execute();
    }
}
