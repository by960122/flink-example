package com.example.watermark.custom;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;

import lombok.extern.slf4j.Slf4j;

/***
 * @author: BYDylan
 * @date: 2024-09-22 11:35:27
 * @description: 自定义水位线-断点: 会不停地检测 onEvent()中的事件, 当发现带有水位线信息的事件时, 就立即发出水位线。我们把发射水位线的逻辑写在 onEvent 方法当中即可
 */
@Slf4j
public class CustomPuntuatedWatermarkGenerator<T> implements WatermarkGenerator<T> {

    // 乱序等待时间
    private long delayTs;
    // 用来保存 当前为止 最大的事件时间
    private long maxTs;

    public CustomPuntuatedWatermarkGenerator(long delayTs) {
        this.delayTs = delayTs;
        this.maxTs = Long.MIN_VALUE + this.delayTs + 1;
    }

    /**
     * 每条数据来, 都会调用一次: 用来提取最大的事件时间, 保存下来,并发射watermark
     */
    @Override
    public void onEvent(T event, long eventTimestamp, WatermarkOutput output) {
        maxTs = Math.max(maxTs, eventTimestamp);
        output.emitWatermark(new Watermark(maxTs - delayTs - 1));
        log.info("onEvent: {}, watermark: {}", maxTs, (maxTs - delayTs - 1));
    }

    /**
     * 周期性调用: 不需要
     *
     * @param output
     */
    @Override
    public void onPeriodicEmit(WatermarkOutput output) {

    }
}
