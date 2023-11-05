package com.example.transformation;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import com.example.model.WaterSensor;

/***
 * @author: BYDylan
 * @date: 2024-09-01 23:27:08
 * @description: 转换算子: filter, true保留, false过滤掉
 */
public class FilterDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<WaterSensor> sensorDS = env.fromElements(
                new WaterSensor("s1", 1L, 1),
                new WaterSensor("s1", 11L, 11),
                new WaterSensor("s2", 2L, 2),
                new WaterSensor("s3", 3L, 3)
        );
        SingleOutputStreamOperator<WaterSensor> filter =
            sensorDS.filter((FilterFunction<WaterSensor>)value -> "s1".equals(value.getId()));
        // SingleOutputStreamOperator<WaterSensor> filter = sensorDS.filter(new FilterFunctionImpl("s1"));
        filter.print();
        env.execute();
    }

    /**
     * udf函数
     */
    public static class FilterFunctionImpl implements FilterFunction<WaterSensor> {

        public String id;

        FilterFunctionImpl(String id) {
            this.id = id;
        }

        @Override
        public boolean filter(WaterSensor value) {
            return this.id.equals(value.getId());
        }
    }
}
