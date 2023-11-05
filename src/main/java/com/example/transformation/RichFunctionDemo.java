package com.example.transformation;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import lombok.extern.slf4j.Slf4j;

/***
 * @author: BYDylan
 * @date: 2024-09-01 23:40:09
 * @description: 富函数 多了生命周期管理方法： open(): 每个子任务, 在启动时, 调用一次, 是 Rich Function 的初始化方法, 也就是会开启一个算子的生命周期 close():每个子任务, 在结束时,
 *               调用一次, 是生命周期中的最后一个调用的方法，类似于结束方法。一般用来做一些清理工作 => 如果是flink程序异常挂掉, 不会调用close => 如果是正常调用 cancel命令, 可以close
 *               2、多了一个 运行时上下文 可以获取一些运行时的环境信息, 比如 子任务编号、名称、其他的.....
 */
@Slf4j
public class RichFunctionDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
            StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.setParallelism(2);

        DataStreamSource<String> source = env.socketTextStream("hadoop102", 7777);
        SingleOutputStreamOperator<Integer> map = source.map(new RichMapFunction<>() {
            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                log.info("task open: {}, name: {}", getRuntimeContext().getIndexOfThisSubtask(),
                    getRuntimeContext().getTaskNameWithSubtasks());
            }

            @Override
            public void close() throws Exception {
                super.close();
                log.info("task close: {}, name: {}", getRuntimeContext().getIndexOfThisSubtask(),
                    getRuntimeContext().getTaskNameWithSubtasks());
            }

            @Override
            public Integer map(String value) {
                return Integer.parseInt(value) + 1;
            }
        });
        map.print();
        env.execute();
    }
}
