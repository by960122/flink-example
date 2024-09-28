package com.example.process;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import com.example.functions.StrToWaterSensorMapFunction;
import com.example.model.WaterSensor;

/***
 * @author: BYDylan
 * @date: 2024-09-22 13:05:34
 * @description: 实时统计一段时间内的出现次数最多的水位。 例如, 统计最近 10 秒钟内出现次数最多的两个水位, 并且每 5 秒钟更新一次 思路二： 使用 KeyedProcessFunction实现
 *               1、按照vc做keyby, 开窗, 分别count ==》 增量聚合, 计算 count ==》 全窗口, 对计算结果 count值封装 , 带上 窗口结束时间的 标签 ==》
 *               为了让同一个窗口时间范围的计算结果到一起去
 *
 *               2、对同一个窗口范围的count值进行处理： 排序、取前N个 =》 按照 windowEnd做keyby =》 使用process, 来一条调用一次, 需要先存, 分开存,
 *               用HashMap,key=windowEnd,value=List =》 使用定时器, 对 存起来的结果 进行 排序、取前N个
 */
public class KeyedProcessFunctionTopNDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        SingleOutputStreamOperator<WaterSensor> sensorDS = env
                .socketTextStream("hadoop102", 7777)
            .map(new StrToWaterSensorMapFunction()).assignTimestampsAndWatermarks(WatermarkStrategy
                                .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                .withTimestampAssigner((element, ts) -> element.getTs() * 1000L));

        // 1. 按照 vc 分组、开窗、聚合（增量计算+全量打标签）
        // 开窗聚合后, 就是普通的流, 没有了窗口信息, 需要自己打上窗口的标记 windowEnd
        SingleOutputStreamOperator<Tuple3<Integer, Integer, Long>> windowAgg = sensorDS.keyBy(WaterSensor::getVc)
                .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
            .aggregate(new VcCountAgg(), new WindowResult());

        // 2. 按照窗口标签（窗口结束时间）keyby, 保证同一个窗口时间范围的结果, 到一起去。排序、取TopN
        windowAgg.keyBy(r -> r.f2)
                .process(new TopN(2))
                .print();
        env.execute();
    }


    public static class VcCountAgg implements AggregateFunction<WaterSensor, Integer, Integer> {

        @Override
        public Integer createAccumulator() {
            return 0;
        }

        @Override
        public Integer add(WaterSensor value, Integer accumulator) {
            return accumulator + 1;
        }

        @Override
        public Integer getResult(Integer accumulator) {
            return accumulator;
        }

        @Override
        public Integer merge(Integer a, Integer b) {
            return null;
        }
    }


    /**
     * 泛型如下： 第一个：输入类型 = 增量函数的输出 count值, Integer 第二个：输出类型 = Tuple3(vc, count, windowEnd), 带上 窗口结束时间 的标签 第三个：key类型 , vc,
     * Integer 第四个：窗口类型
     */
    private static class WindowResult
        extends ProcessWindowFunction<Integer, Tuple3<Integer, Integer, Long>, Integer, TimeWindow> {

        @Override
        public void process(Integer key, Context context, Iterable<Integer> elements, Collector<Tuple3<Integer, Integer, Long>> out) throws Exception {
            // 迭代器里面只有一条数据, next一次即可
            Integer count = elements.iterator().next();
            long windowEnd = context.window().getEnd();
            out.collect(Tuple3.of(key, count, windowEnd));
        }
    }

    private static class TopN extends KeyedProcessFunction<Long, Tuple3<Integer, Integer, Long>, String> {
        // 存不同窗口的 统计结果, key=windowEnd, value=list数据
        private Map<Long, List<Tuple3<Integer, Integer, Long>>> dataListMap;
        // 要取的Top数量
        private int threshold;

        TopN(int threshold) {
            this.threshold = threshold;
            dataListMap = new HashMap<>();
        }

        @Override
        public void processElement(Tuple3<Integer, Integer, Long> value, Context ctx, Collector<String> out) throws Exception {
            // 进入这个方法, 只是一条数据, 要排序, 得到齐才行 ===》 存起来, 不同窗口分开存
            // 1. 存到HashMap中
            Long windowEnd = value.f2;
            if (dataListMap.containsKey(windowEnd)) {
                List<Tuple3<Integer, Integer, Long>> dataList = dataListMap.get(windowEnd);
                dataList.add(value);
            } else {
                List<Tuple3<Integer, Integer, Long>> dataList = new ArrayList<>();
                dataList.add(value);
                dataListMap.put(windowEnd, dataList);
            }
            // 2. 注册一个定时器, windowEnd+1ms即可
            // 同一个窗口范围, 应该同时输出, 只不过是一条一条调用processElement方法, 只需要延迟1ms即可
            ctx.timerService().registerEventTimeTimer(windowEnd + 1);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            super.onTimer(timestamp, ctx, out);
            // 定时器触发, 同一个窗口范围的计算结果攒齐了, 开始 排序、取TopN
            Long windowEnd = ctx.getCurrentKey();
            // 1. 排序
            List<Tuple3<Integer, Integer, Long>> dataList = dataListMap.get(windowEnd);
            dataList.sort((o1, o2) -> {
                // 降序, 后 减 前
                return o2.f1 - o1.f1;
            });
            // 2. 取TopN
            StringBuilder outStr = new StringBuilder();
            outStr.append("================================\n");
            // 遍历 排序后的 List, 取出前 threshold 个, 考虑可能List不够2个的情况 ==》 List中元素的个数 和 2 取最小值
            for (int i = 0; i < Math.min(threshold, dataList.size()); i++) {
                Tuple3<Integer, Integer, Long> vcCount = dataList.get(i);
                outStr.append("Top").append(i + 1).append("\n");
                outStr.append("vc=").append(vcCount.f0).append("\n");
                outStr.append("count=").append(vcCount.f1).append("\n");
                outStr.append("窗口结束时间=").append(vcCount.f2).append("\n");
                outStr.append("================================\n");
            }
            // 用完的List, 及时清理, 节省资源
            dataList.clear();
            out.collect(outStr.toString());
        }
    }
}
