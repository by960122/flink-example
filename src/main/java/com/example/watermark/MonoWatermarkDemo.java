package com.example.watermark;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
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
 * @date: 2024-09-22 11:25:33
 * @description: 内置-水位线-有序流
 */
@Slf4j
public class MonoWatermarkDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<WaterSensor> sensorDS =
            env.fromElements(new WaterSensor("s1", 1L, 1), new WaterSensor("s2", 2L, 2), new WaterSensor("s3", 3L, 3));
        // 1.定义Watermark策略
        WatermarkStrategy<WaterSensor> watermarkStrategy = WatermarkStrategy
            // 1.1 指定watermark生成：升序的watermark, 没有等待时间
            .<WaterSensor>forMonotonousTimestamps()
            // 1.2 指定 时间戳分配器, 从数据中提取
            .withTimestampAssigner((SerializableTimestampAssigner<WaterSensor>)(element, recordTimestamp) -> {
                // 返回的时间戳, 要 毫秒
                log.info("data: {}, recordTs: {}", element, recordTimestamp);
                return element.getTs() * 1000L;
            });
        // 2. 指定 watermark策略
        SingleOutputStreamOperator<WaterSensor> sensorDSwithWatermark =
            sensorDS.assignTimestampsAndWatermarks(watermarkStrategy);
        sensorDSwithWatermark.keyBy(WaterSensor::getId).window(TumblingEventTimeWindows.of(Time.seconds(10)))
            .process(new WaterSensorProcessWindowFunction()).print();
        env.execute();
    }
}
