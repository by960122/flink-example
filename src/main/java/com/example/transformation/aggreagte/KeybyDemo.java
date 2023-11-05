package com.example.transformation.aggreagte;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import com.example.model.WaterSensor;

/***
 * @author: BYDylan
 * @date: 2024-09-01 23:43:53
 * @description: 聚合算子, keyby: 按照id分组 要点: 1、返回的是 一个 KeyedStream, 键控流 2、keyby不是 转换算子, 只是对数据进行重分区, 不能设置并行度 3、分组 与 分区 的关系:
 *               1) keyby是对数据分组, 保证 相同key的数据 在同一个分区（子任务) 2) 分区: 一个子任务可以理解为一个分区, 一个分区（子任务)中可以存在多个分组(key) 类似的还有: sum():
 *               在输入流上, 对指定的字段做叠加求和的操作 min(): 在输入流上, 对指定的字段求最小值 max(): 在输入流上, 对指定的字段求最大值 minBy(): 与 min()类似,
 *               在输入流上针对指定字段求最小值. 不同的是, min()只计算指定字段的最小值, 其他字段会保留最初第一个数据的值；而 minBy()则会返回包含字段最小值的整条数据。 maxBy(): 与
 *               max()类似, 在输入流上针对指定字段求最大值. 两者区别与min()/minBy()完全一致
 */
public class KeybyDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<WaterSensor> sensorDS = env.fromElements(new WaterSensor("s1", 1L, 1),
            new WaterSensor("s1", 11L, 11), new WaterSensor("s2", 2L, 2), new WaterSensor("s3", 3L, 3));
        // 不设置并行度好观察一些.
        KeyedStream<WaterSensor, String> sensorKS =
            sensorDS.keyBy((KeySelector<WaterSensor, String>)WaterSensor::getId);
        sensorKS.print();
        env.execute();
    }
}
