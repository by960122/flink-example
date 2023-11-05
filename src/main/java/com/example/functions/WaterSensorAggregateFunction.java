package com.example.functions;

import org.apache.flink.api.common.functions.AggregateFunction;

import com.example.model.WaterSensor;

import lombok.extern.slf4j.Slf4j;

/**
 * @author: BYDylan
 * @date: 2024-09-21 16:12:31
 * @description: 窗口-增量聚合函数 第一个类型： 输入数据的类型 第二个类型： 累加器的类型, 存储的中间计算结果的类型 第三个类型： 输出的类型
 */
@Slf4j
public class WaterSensorAggregateFunction implements AggregateFunction<WaterSensor, WaterSensor, WaterSensor> {

    // 创建累加器, 初始化累加器
    @Override
    public WaterSensor createAccumulator() {
        log.info("创建累加器");
        return new WaterSensor("accumulator", 0L, 0);
    }

    // 聚合逻辑
    @Override
    public WaterSensor add(WaterSensor in, WaterSensor accumulator) {
        log.info("调用add方法,in: {}", in);
        accumulator.setTs(in.getTs());
        accumulator.setVc(accumulator.getVc() + in.getVc());
        return accumulator;
    }

    // 获取最终结果, 窗口触发时输出
    @Override
    public WaterSensor getResult(WaterSensor accumulator) {
        log.info("调用getResult方法");
        return accumulator;
    }

    @Override
    public WaterSensor merge(WaterSensor a, WaterSensor b) {
        log.info("调用merge方法(只有会话窗口才会用到)");
        return null;
    }
}
