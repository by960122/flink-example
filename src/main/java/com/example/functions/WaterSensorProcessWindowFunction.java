package com.example.functions;

import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import com.example.model.WaterSensor;

import lombok.extern.slf4j.Slf4j;

/**
 * @author: BYDylan
 * @date: 2024-09-21 16:12:31
 * @description: 全窗口函数 第一个参数: 输入的类型 第二个参数: 输出的类型 第三个参数: key的类型 低俗个参数: 窗口
 */
@Slf4j
public class WaterSensorProcessWindowFunction extends ProcessWindowFunction<WaterSensor, String, String, TimeWindow> {

    /**
     * 全窗口函数计算逻辑: 窗口触发时才会调用一次, 统一计算窗口的所有数据
     *
     * @param key 分组的key
     * @param context 上下文
     * @param in 存的数据
     * @param out 采集器
     */
    @Override
    public void process(String key, Context context, Iterable<WaterSensor> in, Collector<String> out) throws Exception {
        long startTs = context.window().getStart();
        long endTs = context.window().getEnd();
        String windowStart = DateFormatUtils.format(startTs, "yyyy-MM-dd HH:mm:ss.SSS");
        String windowEnd = DateFormatUtils.format(endTs, "yyyy-MM-dd HH:mm:ss.SSS");
        long count = in.spliterator().estimateSize();
        out.collect("key=" + key + "的窗口[" + windowStart + "," + windowEnd + ")包含" + count + "条数据===>" + in.toString());

    }
}
