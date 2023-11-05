package com.example.functions;

import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import com.example.model.WaterSensor;

import lombok.extern.slf4j.Slf4j;

/**
 * @author: BYDylan
 * @date: 2024-09-21 16:12:31
 * @description: 全窗口函数
 */
@Slf4j
public class WaterSensorWindowFunction implements WindowFunction<WaterSensor, String, String, TimeWindow> {

    /**
     * @param key 分组的key
     * @param window 窗口对象
     * @param in 存的数据
     * @param out 采集器
     */
    @Override
    public void apply(String key, TimeWindow window, Iterable<WaterSensor> in, Collector<String> out) {}
}
