package com.example.functions;

import org.apache.flink.api.common.functions.MapFunction;

import com.example.model.WaterSensor;

public class WaterSensorIdMapFunction implements MapFunction<WaterSensor, String> {
    @Override
    public String map(WaterSensor value) throws Exception {
        return value.getId();
    }
}
