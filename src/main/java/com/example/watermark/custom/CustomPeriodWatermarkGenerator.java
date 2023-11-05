package com.example.watermark.custom;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;

import lombok.extern.slf4j.Slf4j;

/***
 * @author: BYDylan
 * @date: 2024-09-22 11:32:47
 * @description: 自定义水位线-周期: 通过 onEvent()观察判断输入的事件，而在 onPeriodicEmit()里发出水位线
 */
@Slf4j
public class CustomPeriodWatermarkGenerator<T> implements WatermarkGenerator<T> {

    // 乱序等待时间
    private long delayTs;
    // 用来保存 当前为止 最大的事件时间
    private long maxTs;

    public CustomPeriodWatermarkGenerator(long delayTs) {
        this.delayTs = delayTs;
        this.maxTs = Long.MIN_VALUE + this.delayTs + 1;
    }

    /**
     * 每条数据来, 都会调用一次： 用来提取最大的事件时间, 保存下来
     *
     * @param event
     * @param eventTimestamp 提取到的数据的 事件时间
     * @param output
     */
    @Override
    public void onEvent(T event, long eventTimestamp, WatermarkOutput output) {
        maxTs = Math.max(maxTs, eventTimestamp);
        log.info("onEvent: {}", maxTs);
    }

    /**
     * 周期性调用: 发射 watermark
     *
     * @param output
     */
    @Override
    public void onPeriodicEmit(WatermarkOutput output) {
        output.emitWatermark(new Watermark(maxTs - delayTs - 1));
        log.info("onPeriodicEmit: {}", maxTs - delayTs - 1);
    }
}
