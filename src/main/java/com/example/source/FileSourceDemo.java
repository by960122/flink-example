package com.example.source;

import java.time.Duration;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/***
 * @author: BYDylan
 * @date: 2024-09-01 22:56:12
 * @description: 从文件中读取数据
 */
public class FileSourceDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        FileSource<String> fileSource = FileSource
            .forRecordStreamFormat(new TextLineInputFormat(), new Path("C:\\WorkSpace\\flink-example\\doc\\data.txt"))
            // 持续监控文件变化, 每秒检查一次
            .monitorContinuously(Duration.ofSeconds(1))
                .build();
        env.fromSource(fileSource, WatermarkStrategy.noWatermarks(), "filesource").print();
        env.execute("file source");
    }
}