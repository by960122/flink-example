import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * Author:BYDylan
 * Date:2020/5/4
 * Description:手工通过socket实时产生一些单词,使用flink实时接收数据,对指定时间窗口内(例如：2秒)的数据进行聚合统计，并且把时间窗口内计算的结果打印出来
 */
public class StreamingWindowWordCount {
    public static void main(String[] args) throws Exception {
//        端口号,默认没有指定的时候使用9000,如果想指定,可以通过 --port 来指定
        int port;
        try {
            ParameterTool parameterTool = ParameterTool.fromArgs(args);
//            port = parameterTool.getInt("port");
            port = 9000;
        } catch (Exception e) {
            System.err.println("No Port set,use default 9000");
            port = 9000;
        }
//        1: 获取一个运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        2: 指定数据源(socket)
        DataStreamSource<String> text = env.socketTextStream("127.0.0.1", port, "\n");
//        3：对数进行计算
        DataStream<WordWithCount> windowCount = text.flatMap(new FlatMapFunction<String, WordWithCount>() {
            public void flatMap(String value, Collector<WordWithCount> out) throws Exception {
                String[] split = value.split("\\s");
                for (String word : split) {
                    out.collect(new WordWithCount(word, 1L));
                }
            }
        }).keyBy("word")
                .timeWindow(Time.seconds(1), Time.seconds(1))//第一个参数是指窗口的大小，第二个参数是指滑动的间隔
                //.sum("count");//效果和下面的reduce函数一致
                .reduce((ReduceFunction<WordWithCount>) (a, b) -> new WordWithCount(a.word, a.count + b.count));
//        4: 把数据打印到控制台
        windowCount.print().setParallelism(1);
//        5: 提交(执行)任务
        env.execute("Socket window count");
    }

    public static class WordWithCount{
        String word;
        long count;

        public String getWord() {
            return word;
        }

        public void setWord(String word) {
            this.word = word;
        }

        public long getCount() {
            return count;
        }

        public void setCount(long count) {
            this.count = count;
        }
        public WordWithCount() {
        }

        WordWithCount(String word, long count) {
            this.word = word;
            this.count = count;
        }

        @Override
        public String toString() {
            return word + " : " + count;
        }
    }
}
