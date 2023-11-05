package com.example;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/***
 * @author: BYDylan
 * @date: 2024-09-22 11:20:22
 * @description: 创建执行环境
 */
public class EnvDemo {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set(RestOptions.BIND_PORT, "8082");

        StreamExecutionEnvironment env = StreamExecutionEnvironment
            // .getExecutionEnvironment(); // 自动识别是 远程集群, 还是idea本地环境
            // .createLocalEnvironment()
            // .createRemoteEnvironment("hadoop102", 8081, "/xxx");
            .getExecutionEnvironment(conf); // conf对象可以去修改一些参数
        // 流批一体：代码api是同一套, 可以指定为 批, 也可以指定为 流
        // 默认 STREAMING
        // 一般不在代码写死, 提交时 参数指定：-Dexecution.runtime-mode=BATCH
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);


        env.socketTextStream("hadoop102", 7777).flatMap((String value, Collector<Tuple2<String, Integer>> out) -> {
                            String[] words = value.split(" ");
                            for (String word : words) {
                                out.collect(Tuple2.of(word, 1));
                            }
                        }
                )
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(value -> value.f0)
                .sum(1)
                .print();
                // 默认 env.execute()触发一个flink job, 一个main方法可以调用多个execute, 但是没意义, 指定到第一个就会阻塞住
                // env.executeAsync(), 异步触发, 不阻塞, 一个main方法里 executeAsync()个数 = 生成的flink job数
        env.execute();
    }
}
