package com.example.state;

import java.time.Duration;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import com.example.functions.StrToWaterSensorMapFunction;
import com.example.model.WaterSensor;

/***
 * @author: BYDylan
 * @date: 2024-09-22 17:29:52
 * @description: 状态生存时间: 状态创建的时候, 设置 失效时间 = 当前时间 + TTL；之后如果有对状态的访 问和修改, 我们可以再对失效时间进行更新；当设置的清除条件被触发时（比如, 状态被访 问的时候,
 *               或者每隔一段时间扫描一次失效状态）, 就可以判断状态是否失效、从而进行清除 了。
 */
public class StateTTLDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        SingleOutputStreamOperator<WaterSensor> sensorDS = env.socketTextStream("hadoop102", 7777)
            .map(new StrToWaterSensorMapFunction()).assignTimestampsAndWatermarks(
                WatermarkStrategy.<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                    .withTimestampAssigner((element, ts) -> element.getTs() * 1000L));

        sensorDS.keyBy(WaterSensor::getId).process(new KeyedProcessFunction<String, WaterSensor, String>() {
            ValueState<Integer> lastVcState;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                // 1.创建 StateTtlConfig
                StateTtlConfig stateTtlConfig = StateTtlConfig.newBuilder(Time.seconds(5)) // 过期时间5s
                    // .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite) // 状态 创建和写入（更新） 更新 过期时间
                    .setUpdateType(StateTtlConfig.UpdateType.OnReadAndWrite) // 状态 读取、创建和写入（更新） 更新 过期时间
                    .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired) // 不返回过期的状态值
                    .build();

                // 2.状态描述器 启用 TTL
                ValueStateDescriptor<Integer> stateDescriptor = new ValueStateDescriptor<>("lastVcState", Types.INT);
                stateDescriptor.enableTimeToLive(stateTtlConfig);
                this.lastVcState = getRuntimeContext().getState(stateDescriptor);
            }

            @Override
            public void processElement(WaterSensor value, Context ctx, Collector<String> out) throws Exception {
                // 先获取状态值, 打印 ==》 读取状态
                Integer lastVc = lastVcState.value();
                out.collect("key=" + value.getId() + ",状态值=" + lastVc);
                // 如果水位大于10, 更新状态值 ===》 写入状态
                if (value.getVc() > 10) {
                    lastVcState.update(value.getVc());
                }
            }
        }).print();
        env.execute();
    }
}
