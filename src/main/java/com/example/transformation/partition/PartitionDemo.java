package com.example.transformation.partition;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/***
 * @author: BYDylan
 * @date: 2024-09-04 23:26:03
 * @description: 分区介绍 物理分区算子: 随机分配(Random)、轮询分配(Round-Robin)、重缩放(Rescale)和广播(Broadcast) 随机分区(stream.shuffle()):
 *               将数据随机地分配到下游算子的并行任务中去 轮询分区(stream.rebalance()): 按照先后顺序将数据做依次分发 重缩放分区(stream.rescale()):
 *               重缩放分区和轮询分区非常相似。当调用 rescale()方法时, 其实底层也是使用 Round Robin算法进行轮询, 但是只会将数据轮询发送到下游并行任务的一部分中。rescale的做法是分成小团体,
 *               发牌人只给自己团体内的所有人轮流发牌 广播(stream.broadcast()): 经过广播之后, 数据会在不同的分区都保留一份, 可能进行重复处理 全局分区(stream.global()):
 *               会将所有的输入流数据都发送到下游算子的第一个并行子任务中去。这就相当于强行让下游任务并行度变成了 1, 所以使用这个操作需要非常谨慎, 可能对程序造成很大的压力
 */
public class PartitionDemo {
    public static void main(String[] args) throws Exception {
        // StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamExecutionEnvironment env =
            StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        env.setParallelism(2);

        DataStreamSource<String> socketDS = env.socketTextStream("hadoop102", 7777);

        // shuffle随机分区: random.nextInt(下游算子并行度)
        // socketDS.shuffle().print();

        // rebalance轮询：nextChannelToSendTo = (nextChannelToSendTo + 1) % 下游算子并行度
        // 如果是 数据源倾斜的场景, source后, 调用rebalance, 就可以解决 数据源的 数据倾斜
        // socketDS.rebalance().print();

        // rescale缩放： 实现轮询, 局部组队, 比rebalance更高效
        // socketDS.rescale().print();

        // broadcast 广播：发送给下游所有的子任务
        // socketDS.broadcast().print();

        // global 全局： 全部发往 第一个子任务
        // return 0;
        socketDS.global().print();

        // keyby: 按指定key去发送, 相同key发往同一个子任务
        // one-to-one: Forward分区器
        // 总结： Flink提供了7种分区器 + 1种自定义
        env.execute();
    }
}