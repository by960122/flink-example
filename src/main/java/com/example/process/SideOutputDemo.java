package com.example.process;

import java.time.Duration;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import com.example.functions.StrToWaterSensorMapFunction;
import com.example.model.WaterSensor;

/***
 * @author: BYDylan
 * @date: 2024-09-22 13:09:07
 * @description: 侧输出流: 对每个传感器, 水位超过 10 的输出告警信息
 */
public class SideOutputDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        SingleOutputStreamOperator<WaterSensor> sensorDS = env
                .socketTextStream("hadoop102", 7777)
            .map(new StrToWaterSensorMapFunction()).assignTimestampsAndWatermarks(
                WatermarkStrategy.<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                    .withTimestampAssigner((element, ts) -> element.getTs() * 1000L));
        OutputTag<String> warnTag = new OutputTag<>("warn", Types.STRING);
        SingleOutputStreamOperator<WaterSensor> process =
            sensorDS.keyBy(WaterSensor::getId)
                .process(
                    new KeyedProcessFunction<>() {
                            @Override
                        public void processElement(WaterSensor value, Context ctx, Collector<WaterSensor> out) {
                                // 使用侧输出流告警
                                if (value.getVc() > 10) {
                                    ctx.output(warnTag, "当前水位=" + value.getVc() + ",大于阈值10！！！");
                                }
                                // 主流正常 发送数据
                                out.collect(value);
                            }
                        }
                );
        process.print("主流");
        process.getSideOutput(warnTag).printToErr("warn");
        env.execute();
    }
}
