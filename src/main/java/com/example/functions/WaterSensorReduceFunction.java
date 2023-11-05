package com.example.functions;

import org.apache.flink.api.common.functions.ReduceFunction;

import com.example.model.WaterSensor;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class WaterSensorReduceFunction implements ReduceFunction<WaterSensor> {
    @Override
    public WaterSensor reduce(WaterSensor value1, WaterSensor value2) {
        log.info("调用reduce方法, value1: {}, value2: {}", value1, value2);
        return new WaterSensor(value1.getId(), value2.getTs(), value1.getVc() + value2.getVc());
    }
}
