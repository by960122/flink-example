import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * Author:BYDylan
 * Date:2020/5/5
 * Description:单词计数,读取本地文件,对文件中的单词进行拆分,并且统计每个单词出现的总次数
 */
public class BatchWordCountJava {
    public static void main(String[] args) throws Exception {
//        待读取的目录
        String inputPath = "D:\\WorkSpace\\ideaProject\\flink_example\\doc\\data.txt";
        String outputPath = "D:\\data\\count";
//        获取执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
//        获取数据源(文件)
        DataSet<String> text = env.readTextFile(inputPath);
        DataSet<Tuple2<String, Integer>> wordCount = text.flatMap(new Tokenizer()).groupBy(0).sum(1);
        wordCount.print();
//        通过sink组件把数据写到文件中
//        wordCount.writeAsCsv(outputPath, "\n", " ").setParallelism(1);
//        执行任务
//        env.execute("batch word count");
    }

    public static class Tokenizer implements FlatMapFunction<String, Tuple2<String, Integer>> {
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
            String[] split = value.toLowerCase().split("\\W+");
            for (String word : split) {
                if (word.length() > 0) {
                    out.collect(new Tuple2<>(word, 1));
                }
            }
        }
    }
}
