package com.example.source;

import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import com.example.functions.StrToWaterSensorMapFunction;
import com.example.model.WaterSensor;

/***
 * @author: BYDylan
 * @date: 2024-09-21 10:50:54
 * @description: socket source
 */
public class SocketSourceDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        SingleOutputStreamOperator<WaterSensor> sensorDS =
            env.socketTextStream("hadoop102", 7777).map(new StrToWaterSensorMapFunction());
        sensorDS.print().setParallelism(1);
        env.execute("Socket window count");
    }
}
