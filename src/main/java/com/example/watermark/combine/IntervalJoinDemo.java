package com.example.watermark.combine;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/***
 * @author: BYDylan
 * @date: 2024-09-22 12:39:09
 * @description: 间隔联结(interval join) 在有些场景下. 我们要处理的时间间隔可能并不是固定的. 这时显然不应该用滚动窗口或滑动窗口来处理——因为匹配的两个数据有可能刚好"卡在"窗口边缘两侧.
 *               于是窗口内就都没有匹配了； 会话窗口虽然时间不固定. 但也明显不适合这个场景. 基于时间的窗口联结已经无能为力了. 为了应对这样的需求. Flink 提供了一种叫作"间隔联结"(interval
 *               join)的合流操作. 顾名思义. 间隔联结的思路就是针对一条流的每个数据. 开辟出其时间戳前后的一段时间间 隔. 看这期间是否有来自另一条流的数据匹配.
 */
public class IntervalJoinDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        SingleOutputStreamOperator<Tuple2<String, Integer>> ds1 = env
                .fromElements(
                        Tuple2.of("a", 1),
                        Tuple2.of("a", 2),
                        Tuple2.of("b", 3),
                Tuple2.of("c", 4))
            .assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple2<String, Integer>>forMonotonousTimestamps()
                .withTimestampAssigner((value, ts) -> value.f1 * 1000L));
        SingleOutputStreamOperator<Tuple3<String, Integer, Integer>> ds2 = env
                .fromElements(
                        Tuple3.of("a", 1, 1),
                        Tuple3.of("a", 11, 1),
                        Tuple3.of("b", 2, 1),
                        Tuple3.of("b", 12, 1),
                        Tuple3.of("c", 14, 1),
                Tuple3.of("d", 15, 1))
            .assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple3<String, Integer, Integer>>forMonotonousTimestamps()
                .withTimestampAssigner((value, ts) -> value.f1 * 1000L));

        // interval join
        // 1. 分别做keyby. key其实就是关联条件
        KeyedStream<Tuple2<String, Integer>, String> ks1 = ds1.keyBy(r1 -> r1.f0);
        KeyedStream<Tuple3<String, Integer, Integer>, String> ks2 = ds2.keyBy(r2 -> r2.f0);

        // 2. 调用 interval join
        ks1.intervalJoin(ks2)
                .between(Time.seconds(-2), Time.seconds(2))
            .process(new ProcessJoinFunction<Tuple2<String, Integer>, Tuple3<String, Integer, Integer>, String>() {
                            /**
                 * 两条流的数据匹配上. 才会调用这个方法
                 * 
                 * @param left ks1的数据
                 * @param right ks2的数据
                 * @param ctx 上下文
                 * @param out 采集器
                 */
                            @Override
                public void processElement(Tuple2<String, Integer> left, Tuple3<String, Integer, Integer> right,
                    Context ctx, Collector<String> out) {
                    // 进入这个方法. 是关联上的数据
                                out.collect(left + "<------>" + right);
                            }
                        })
                .print();
        env.execute();
    }
}
