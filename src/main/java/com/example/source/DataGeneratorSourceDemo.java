package com.example.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/***
 * @author: BYDylan
 * @date: 2024-09-01 23:02:07
 * @description: 从数据生成器读取数据 数据生成器Source, 四个参数: 第一个： GeneratorFunction接口, 需要实现, 重写map方法, 输入类型固定是Long 第二个： long类型,
 *               自动生成的数字序列（从0自增）的最大值(小于), 达到这个值就停止了 第三个： 限速策略, 比如 每秒生成几条数据 第四个： 返回的类型
 */
public class DataGeneratorSourceDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 每个并行度的最大数是count/并行度(2) = 50, 起始范围分别是0-49,50-100
        env.setParallelism(2);

        DataGeneratorSource<String> dataGeneratorSource =
            new DataGeneratorSource<>((GeneratorFunction<Long, String>)value -> "Number:" + value, 100,
                RateLimiterStrategy.perSecond(1), Types.STRING);

        env.fromSource(dataGeneratorSource, WatermarkStrategy.noWatermarks(), "data-generator").print();
        env.execute("data-generator demo");
    }
}
