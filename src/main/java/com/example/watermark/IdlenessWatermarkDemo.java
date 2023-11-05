package com.example.watermark;

import java.time.Duration;

import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import com.example.transformation.partition.CustomPartitioner;

/***
 * @author: BYDylan
 * @date: 2024-09-22 11:44:55
 * @description: 空闲等待 在多个上游并行任务中, 如果有其中一个没有数据, 由于当前 Task 是以最小的那个作为当前任务的事件时钟, 就会导致当前 Task 的水位线无法推进, 就可能导致窗口无法触发.
 *               这时候可以设置空闲等待.
 */
public class IdlenessWatermarkDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        // 自定义分区器: 数据%分区数, 只输入奇数, 都只会去往map的一个子任务
        SingleOutputStreamOperator<Integer> socketDS = env
                .socketTextStream("hadoop102", 7777)
            .partitionCustom(new CustomPartitioner(), r -> r).map(Integer::parseInt)
            .assignTimestampsAndWatermarks(WatermarkStrategy.<Integer>forMonotonousTimestamps()
                .withTimestampAssigner((r, ts) -> r * 1000L).withIdleness(Duration.ofSeconds(5)) // 空闲等待5s
                );
        // 分成两组: 奇数一组, 偶数一组, 开10s的事件时间滚动窗口
        socketDS.keyBy(r -> r % 2)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .process(new ProcessWindowFunction<Integer, String, Integer, TimeWindow>() {
                    @Override
                public void process(Integer key, Context context, Iterable<Integer> in, Collector<String> out) {
                        long startTs = context.window().getStart();
                        long endTs = context.window().getEnd();
                        String windowStart = DateFormatUtils.format(startTs, "yyyy-MM-dd HH:mm:ss.SSS");
                        String windowEnd = DateFormatUtils.format(endTs, "yyyy-MM-dd HH:mm:ss.SSS");
                    long count = in.spliterator().estimateSize();
                    out.collect("key=" + key + "的窗口[" + windowStart + "," + windowEnd + ")包含" + count + "条数据===>"
                        + in.toString());
                    }
                })
                .print();
        env.execute();
    }
}
