package com.example.process;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import com.example.functions.StrToWaterSensorMapFunction;
import com.example.model.WaterSensor;

/***
 * @author: BYDylan
 * @date: 2024-09-22 13:00:43
 * @description: 实时统计一段时间内的出现次数最多的水位。 例如, 统计最近 10 秒钟内出现次数最多的两个水位, 并且每 5 秒钟更新一次
 */
public class ProcessAllWindowTopNDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<WaterSensor> sensorDS = env
                .socketTextStream("hadoop102", 7777)
            .map(new StrToWaterSensorMapFunction()).assignTimestampsAndWatermarks(
                WatermarkStrategy.<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                    .withTimestampAssigner((element, ts) -> element.getTs() * 1000L));

        // 最近10秒= 窗口长度, 每5秒输出 = 滑动步长
        // 思路一：所有数据到一起, 用hashmap存, key=vc, value=count值
        sensorDS.windowAll(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                .process(new MyTopNPAWF())
                .print();
        env.execute();
    }

    public static class MyTopNPAWF extends ProcessAllWindowFunction<WaterSensor, String, TimeWindow> {

        @Override
        public void process(Context context, Iterable<WaterSensor> elements, Collector<String> out) throws Exception {
            // 定义一个hashmap用来存, key=vc, value=count值
            Map<Integer, Integer> vcCountMap = new HashMap<>();
            // 1.遍历数据, 统计 各个vc出现的次数
            for (WaterSensor element : elements) {
                Integer vc = element.getVc();
                if (vcCountMap.containsKey(vc)) {
                    // 1.1 key存在, 不是这个key的第一条数据, 直接累加
                    vcCountMap.put(vc, vcCountMap.get(vc) + 1);
                } else {
                    // 1.2 key不存在, 初始化
                    vcCountMap.put(vc, 1);
                }
            }

            // 2.对 count值进行排序: 利用List来实现排序
            List<Tuple2<Integer, Integer>> datas = new ArrayList<>();
            for (Integer vc : vcCountMap.keySet()) {
                datas.add(Tuple2.of(vc, vcCountMap.get(vc)));
            }
            // 对List进行排序, 根据count值 降序
            datas.sort((o1, o2) -> {
                // 降序, 后 减 前
                return o2.f1 - o1.f1;
            });
            // 3.取出 count最大的2个 vc
            StringBuilder outStr = new StringBuilder();
            outStr.append("================================\n");
            // 遍历 排序后的 List, 取出前2个, 考虑可能List不够2个的情况 ==》 List中元素的个数 和 2 取最小值
            for (int index = 0; index < Math.min(2, datas.size()); index++) {
                Tuple2<Integer, Integer> vcCount = datas.get(index);
                outStr.append("Top").append(index + 1).append("\n");
                outStr.append("vc=").append(vcCount.f0).append("\n");
                outStr.append("count=").append(vcCount.f1).append("\n");
                outStr.append("窗口结束时间=")
                    .append(DateFormatUtils.format(context.window().getEnd(), "yyyy-MM-dd HH:mm:ss.SSS")).append("\n");
                outStr.append("================================\n");
            }
            out.collect(outStr.toString());
        }
    }
}
