package com.example.sink;

import java.sql.Connection;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

/***
 * @author: BYDylan
 * @date: 2024-09-21 15:22:00
 * @description: 自定义 sink
 */
public class CustomSinkDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        SingleOutputStreamOperator<String> sensorDS = env.socketTextStream("hadoop102", 7777);
        sensorDS.addSink(new MySink());
        env.execute();
    }

    public static class MySink extends RichSinkFunction<String> {
        Connection conn = null;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            // 在这里 创建连接
            // conn = new xxxx
        }

        @Override
        public void close() throws Exception {
            super.close();
            // 做一些清理、销毁连接
        }

        /**
         * sink的核心逻辑, 写出的逻辑就写在这个方法里 这个方法是 来一条数据, 调用一次,所以不要在这里创建 连接对象
         *
         */
        @Override
        public void invoke(String value, Context context) {
        }
    }
}
