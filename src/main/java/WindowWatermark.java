import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Author:BYDylan
 * Date:2020/5/8
 * Description:Watermark
 */
public class WindowWatermark {
    public static void main(String[] args) throws Exception {
        int port = 9001;
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);
//        连接 socket 获取输入的数据
        DataStream<String> text = env.socketTextStream("127.0.0.1", port, "\n");
//        解析输入的数据
        DataStream<Tuple2<String, Long>> inputMap = text.map((MapFunction<String, Tuple2<String, Long>>) value -> {
            String[] arr = value.split(",");
            return new Tuple2<>(arr[0], Long.parseLong(arr[1]));
        });
//        过期了
//        抽取 timestamp 和生成 watermark
//        DataStream<Tuple2<String, Long>> waterMarkStream = inputMap.assignTimestampsAndWatermarks(
//                new AssignerWithPeriodicWatermarks<Tuple2<String, Long>>() {
//                    Long currentMaxTimestamp = 0L;
//                    final Long maxOutOfOrderness = 10000L;// 最大允许的乱序时间是 10s
//                    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
//
//                    //                    定义生成 watermark 的逻辑,默认 100ms 被调用一次
//                    @Nullable
//                    @Override
//                    public Watermark getCurrentWatermark() {
//                        return new Watermark(currentMaxTimestamp - maxOutOfOrderness);
//                    }
//
//                    //                    定义如何提取 timestamp
//                    @Override
//                    public long extractTimestamp(Tuple2<String, Long> element, long previousElementTimestamp) {
//                        long timestamp = element.f1;
//                        currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
//                        System.out.println("key:" + element.f0 + ",eventtime:[" + element.f1 + "|" + sdf.format(element.f1) + "], currentMaxTimestamp:[" + currentMaxTimestamp + "|" +
//                                sdf.format(currentMaxTimestamp) + "],watermark:[" + getCurrentWatermark().getTimestamp() + "|" + sdf.format(getCurrentWatermark().getTimestamp()) + "]");
//                        return timestamp;
//                    }
//                });
        DataStream<Tuple2<String, Long>> waterMarkStream = inputMap.assignTimestampsAndWatermarks(WatermarkStrategy
                .<Tuple2<String, Long>>forBoundedOutOfOrderness(Duration.ofSeconds(100))
                .withTimestampAssigner((event, timestamp) -> event.f1)
        );

//        分组聚合,对 window 内的数据进行排序,保证数据的顺序
        DataStream<String> window = waterMarkStream.keyBy(event -> event.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(3)))// 按 照 消 息 的EventTime 分配窗口,和调用 TimeWindow 效果一样
                .apply((WindowFunction<Tuple2<String, Long>, String, String, TimeWindow>) (tuple, window1, input, out) -> {
                    String key = tuple.toString();
                    List<Long> arrarList = new ArrayList<>();
                    for (Tuple2<String, Long> next : input) {
                        arrarList.add(next.f1);
                    }
                    Collections.sort(arrarList);
                    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS ");
                    String result = key + "," + arrarList.size() + "," +
                            sdf.format(arrarList.get(0)) + "," + sdf.format(arrarList.get(arrarList.size() - 1))
                            + "," + sdf.format(window1.getStart()) + "," +
                            sdf.format(window1.getEnd());
                    out.collect(result);
                });
        window.print();
        env.execute("eventtime-watermark");
    }
}
