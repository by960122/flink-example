package com.example.wc;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.time.Duration;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.util.Collector;

/**
 * @author: BYDylan
 * @date: 2021/4/22
 * @description: 单词计数, 读取本地文件, 对文件中的单词进行拆分, 并且统计每个单词出现的总次数 如果要采用批处理, 执行的时候加个参数就行 bin/flink run
 *               -Dexecution.runtime-mode=BATCH WordCountDemo.jar
 */
public class WordCountFileDemo {
    public static void main(String[] args) throws Exception {
        // 待读取的目录
        String inputPath = "C:\\WorkSpace\\flink-example\\doc\\data.txt";
        String outputPath = "C:\\WorkSpace\\flink-example\\doc\\result.txt";
        // 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 获取数据源(文件)
        // 使用 FileSource.forRecordStreamFormat 创建数据源
        FileSource<String> source = FileSource.forRecordStreamFormat(new TextLineInputFormat(), new Path(inputPath))
            // 持续监控文件变化, 每秒检查一次
                .monitorContinuously(Duration.ofSeconds(1))
                .build();
        DataStream<String> text = env.fromSource(source, WatermarkStrategy.noWatermarks(), "file-source");

        // 使用 flatMap 操作符对单词进行拆分
        DataStream<Tuple2<String, Integer>> wordCount = text.flatMap(new Tokenizer())
                // 按单词分组
                .keyBy(value -> value.f0)
                // 计算每个单词出现的总次数
                .sum(1);
        wordCount.print();
        // 通过sink组件把数据写到文件中
        // wordCount.addSink(new WordCountSinkFunction(new Path(outputPath)));
        // 执行任务
        env.execute("batch word count");
    }

    public static class Tokenizer implements FlatMapFunction<String, Tuple2<String, Integer>> {
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
            String[] split = value.toLowerCase().split("\\W+");
            for (String word : split) {
                if (word.length() > 0) {
                    out.collect(new Tuple2<>(word, 1));
                }
            }
        }
    }

    // 自定义 SinkFunction，用于将单词计数结果写入文本文件
    public static class WordCountSinkFunction implements SinkFunction<Tuple2<String, Integer>> {
        private final Path outputFilePath;

        WordCountSinkFunction(Path outputFilePath) {
            this.outputFilePath = outputFilePath;
        }

        @Override
        public void invoke(Tuple2<String, Integer> value, Context context) {
            // 获取单词和计数
            String word = value.f0;
            int count = value.f1;

            // 写入文本文件
            try (BufferedWriter writer = new BufferedWriter(new FileWriter(outputFilePath.getPath(), true))) {
                writer.write(word + ": " + count + "\n");
            } catch (IOException e) {
                // 处理异常
                System.err.println("Error writing to file: " + e.getMessage());
            }
        }
    }
}
