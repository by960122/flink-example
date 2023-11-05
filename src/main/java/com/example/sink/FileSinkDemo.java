package com.example.sink;

import java.time.Duration;
import java.time.ZoneId;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

/***
 * @author: BYDylan
 * @date: 2024-09-21 10:59:29
 * @description: 输出到文件
 */
public class FileSinkDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 每个目录中，都有 并行度个数的 文件在写入
        env.setParallelism(2);
        // 必须开启checkpoint，否则一直都是 .inprogress
        env.enableCheckpointing(2000, CheckpointingMode.EXACTLY_ONCE);
        DataGeneratorSource<String> dataGeneratorSource =
            new DataGeneratorSource<>((GeneratorFunction<Long, String>)value -> "Number:" + value,
                Long.MAX_VALUE,
                RateLimiterStrategy.perSecond(1000),
                Types.STRING
        );
        DataStreamSource<String> dataGen = env.fromSource(dataGeneratorSource, WatermarkStrategy.noWatermarks(), "data-generator");
        // 输出到文件系统
        FileSink<String> fieSink = FileSink
                .<String>forRowFormat(new Path("f:/tmp"), new SimpleStringEncoder<>("UTF-8"))
            .withOutputFileConfig(OutputFileConfig.builder().withPartPrefix("atguigu-").withPartSuffix(".log").build())
            // 按照目录分桶：如下就是每个小时一个目录
                .withBucketAssigner(new DateTimeBucketAssigner<>("yyyy-MM-dd HH", ZoneId.systemDefault()))
            // 文件滚动策略: 1分钟 或 1M
            .withRollingPolicy(DefaultRollingPolicy.builder().withRolloverInterval(Duration.ofMinutes(1))
                .withMaxPartSize(new MemorySize(1024 * 1024)).build())
                .build();
        dataGen.sinkTo(fieSink);
        env.execute();
    }
}
