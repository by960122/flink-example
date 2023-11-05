package com.example.state;

import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import com.example.model.WaterSensor;

/***
 * @author: BYDylan
 * @date: 2024-09-22 17:35:41
 * @description: 水位超过指定的阈值发送告警, 阈值可以动态修改
 */
public class OperatorBroadcastStateDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        // 数据流
        DataStreamSource<WaterSensor> sensorDS =
            env.fromElements(new WaterSensor("s1", 1L, 1), new WaterSensor("s2", 2L, 2), new WaterSensor("s3", 3L, 3));

        // 配置流（用来广播配置）
        DataStreamSource<String> configDS = env.socketTextStream("hadoop102", 8888);

        // 1. 将 配置流 广播
        MapStateDescriptor<String, Integer> broadcastMapState =
            new MapStateDescriptor<>("broadcast-state", Types.STRING, Types.INT);
        BroadcastStream<String> configBS = configDS.broadcast(broadcastMapState);

        // 2.把 数据流 和 广播后的配置流 connect
        BroadcastConnectedStream<WaterSensor, String> sensorBCS = sensorDS.connect(configBS);

        // 3.调用 process
        sensorBCS.process(new BroadcastProcessFunction<WaterSensor, String, String>() {
            /**
             * 数据流的处理方法: 数据流 只能 读取 广播状态, 不能修改
             * 
             * @param value
             * @param ctx
             * @param out
             * @throws Exception
             */
            @Override
            public void processElement(WaterSensor value, ReadOnlyContext ctx, Collector<String> out) throws Exception {
                // 5.通过上下文获取广播状态, 取出里面的值（只读, 不能修改）
                ReadOnlyBroadcastState<String, Integer> broadcastState = ctx.getBroadcastState(broadcastMapState);
                Integer threshold = broadcastState.get("threshold");
                // 判断广播状态里是否有数据, 因为刚启动时, 可能是数据流的第一条数据先来
                threshold = (threshold == null ? 0 : threshold);
                if (value.getVc() > threshold) {
                    out.collect(value + ",水位超过指定的阈值：" + threshold + "!!!");
                }
            }

            /**
             * 广播后的配置流的处理方法: 只有广播流才能修改 广播状态
             * 
             * @param value
             * @param ctx
             * @param out
             * @throws Exception
             */
            @Override
            public void processBroadcastElement(String value, Context ctx, Collector<String> out) throws Exception {
                // 4. 通过上下文获取广播状态, 往里面写数据
                BroadcastState<String, Integer> broadcastState = ctx.getBroadcastState(broadcastMapState);
                broadcastState.put("threshold", Integer.valueOf(value));
            }
        }).print();
        env.execute();
    }
}
