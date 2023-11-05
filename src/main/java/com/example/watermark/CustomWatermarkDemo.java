package com.example.watermark;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import com.example.functions.WaterSensorProcessWindowFunction;
import com.example.model.WaterSensor;
import com.example.watermark.custom.CustomPuntuatedWatermarkGenerator;

/***
 * @author: BYDylan
 * @date: 2024-09-22 11:38:37
 * @description: 自定义水位线 内置Watermark的生成原理 * 1、都是周期性生成的： 默认200ms * 2、有序流: watermark = 当前最大的事件时间 - 1ms * 3、乱序流: watermark =
 *               当前最大的事件时间 - 延迟时间 - 1ms
 */
public class CustomWatermarkDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 默认周期 200ms
        env.getConfig().setAutoWatermarkInterval(2000);
        DataStreamSource<WaterSensor> sensorDS =
            env.fromElements(new WaterSensor("s1", 1L, 1), new WaterSensor("s2", 2L, 2), new WaterSensor("s3", 3L, 3));
        WatermarkStrategy<WaterSensor> watermarkStrategy = WatermarkStrategy
                // 1.自定义的 周期性生成
            // .<WaterSensor>forGenerator(ctx -> new CustomPeriodWatermarkGenerator<>(3000L))
                // 2.自定义的 断点式生成
            .<WaterSensor>forGenerator(ctx -> new CustomPuntuatedWatermarkGenerator<>(3000L))
            .withTimestampAssigner((element, recordTimestamp) -> element.getTs() * 1000L);
        SingleOutputStreamOperator<WaterSensor> sensorDSwithWatermark = sensorDS.assignTimestampsAndWatermarks(watermarkStrategy);
        sensorDSwithWatermark.keyBy(WaterSensor::getId)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
            .process(new WaterSensorProcessWindowFunction())
                .print();
        env.execute();
    }
}
