package com.example.sql;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

/***
 * @author: BYDylan
 * @date: 2024-09-28 23:51:40
 * @description:
 */
public class MyTableFunctionDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> strDS = env.fromElements("hello flink", "hello world hi", "hello java");
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        Table sensorTable =
            tableEnv.fromDataStream(strDS, Schema.newBuilder().column("words", DataTypes.STRING()).build());
        tableEnv.createTemporaryView("str", sensorTable);
        // 2.注册函数
        tableEnv.createTemporaryFunction("SplitFunction", SplitFunction.class);
        // 3.调用 自定义函数
        // 3.1 交叉联结
        tableEnv
            // 3.1 交叉联结
            // .sqlQuery("select words,word,length from str,lateral table(SplitFunction(words))")
            // 3.2 带 on true 条件的 左联结
            // .sqlQuery("select words,word,length from str left join lateral table(SplitFunction(words)) on true")
            // 重命名侧向表中的字段
            .sqlQuery(
                "select words,newWord,newLength from str left join lateral table(SplitFunction(words))  as T(newWord,newLength) on true")
            .execute().print();

    }

    // 1.继承 TableFunction<返回的类型>
    // 类型标注： Row包含两个字段：word和length
    @FunctionHint(output = @DataTypeHint("ROW<word STRING,length INT>"))
    public static class SplitFunction extends TableFunction<Row> {

        // 返回是 void, 用 collect方法输出
        public void eval(String str) {
            for (String word : str.split(" ")) {
                collect(Row.of(word, word.length()));
            }
        }
    }

}
