package com.example.sink;

import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import com.example.model.WaterSensor;

/***
 * @author: BYDylan
 * @date: 2024-09-21 15:19:59
 * @description: 输出到 mysql
 */
public class MySQLSinkDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<WaterSensor> sensorDS =
            env.fromElements(new WaterSensor("s1", 1L, 1), new WaterSensor("s2", 2L, 2), new WaterSensor("s3", 3L, 3));
        SinkFunction<WaterSensor> jdbcSink = JdbcSink.sink(
                "insert into ws values(?,?,?)",
            (JdbcStatementBuilder<WaterSensor>)(preparedStatement, waterSensor) -> {
                preparedStatement.setString(1, waterSensor.getId());
                preparedStatement.setLong(2, waterSensor.getTs());
                preparedStatement.setInt(3, waterSensor.getVc());
                },
                JdbcExecutionOptions.builder()
                        .withMaxRetries(3) // 重试次数
                        .withBatchSize(100) // 批次的大小：条数
                        .withBatchIntervalMs(3000) // 批次的时间
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                .withUrl(
                    "jdbc:mysql://127.0.0.0:3306/test?serverTimezone=Asia/Shanghai&useUnicode=true&characterEncoding=UTF-8")
                        .withUsername("root")
                .withPassword("By96o122").withConnectionCheckTimeoutSeconds(60)
                        .build()
        );
        sensorDS.addSink(jdbcSink);
        env.execute();
    }
}
