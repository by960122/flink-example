package com.example.watermark;

import java.time.Duration;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import com.example.functions.WaterSensorProcessWindowFunction;
import com.example.model.WaterSensor;

import lombok.extern.slf4j.Slf4j;

/***
 * @author: BYDylan
 * @date: 2024-09-22 11:29:47
 * @description: 内置-水位线-乱序流 在流处理中, 上游任务处理完水位线、时钟改变之后, 要把当前的水位线再次发出, 广播给所有的下游子任务. 而当一个任务接收到多个上游并行任务传递来的水位线时,
 *               应该以最小的那个作为当前任务的事件时钟. 水位线在上下游任务之间的传递, 非常巧妙地避免了分布式系统中没有统一时钟的问题, 每个任务都以“处理完之前所有数据”为标准来确定自己的时钟.
 */
@Slf4j
public class OutOfOrdernessWatermarkDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 演示watermark多并行度下的传递
        // 1、接收到上游多个, 取最小
        // 2、往下游多个发送, 广播
        env.setParallelism(2);
        DataStreamSource<WaterSensor> sensorDS =
            env.fromElements(new WaterSensor("s1", 1L, 1), new WaterSensor("s2", 2L, 2), new WaterSensor("s3", 3L, 3));
        // 1.定义Watermark策略
        WatermarkStrategy<WaterSensor> watermarkStrategy = WatermarkStrategy
            // 1.1 指定watermark生成：乱序的, 等待3s
                .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3))
            // 1.2 指定 时间戳分配器, 从数据中提取
                .withTimestampAssigner(
                        (element, recordTimestamp) -> {
                    // 返回的时间戳, 要 毫秒
                    log.info("data: {}, recordTs: {}", element, recordTimestamp);
                            return element.getTs() * 1000L;
                        });
        // 2. 指定 watermark策略
        SingleOutputStreamOperator<WaterSensor> sensorDSwithWatermark = sensorDS.assignTimestampsAndWatermarks(watermarkStrategy);
        sensorDSwithWatermark.keyBy(WaterSensor::getId)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
            .process(new WaterSensorProcessWindowFunction())
                .print();
        env.execute();
    }
}