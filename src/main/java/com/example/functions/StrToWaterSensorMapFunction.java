package com.example.functions;

import org.apache.flink.api.common.functions.MapFunction;

import com.example.model.WaterSensor;

public class StrToWaterSensorMapFunction implements MapFunction<String, WaterSensor> {
    @Override
    public WaterSensor map(String value) throws Exception {
        String[] datas = value.split(",");
        return new WaterSensor(datas[0], Long.valueOf(datas[1]), Integer.valueOf(datas[2]));
    }
}
