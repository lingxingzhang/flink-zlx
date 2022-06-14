package com.zlx.base;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * 通过socket数据源，通过单词的个数（历史以来）
 */
public class _01_WordCount_java {

    public static void main(String[] args) throws Exception {

        // TODO 1.创建环境变量
//        ExecutionEnvironment executionEnvironment = ExecutionEnvironment.getExecutionEnvironment(); //批处理的入口环境 官方已经不建议使用
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();//流批统一

        /**
         * 本地运行的默认并行度： cup的核数
         */
        env.setParallelism(1);

        // TODO 2.通过source算子生成DataStream
        DataStreamSource<String> source = env.socketTextStream("localhost", 9999);

        // TODO 3.算子处理数据
        // flink 为了让java和scala api 风格统一，专门封装了一个 tuple
        SingleOutputStreamOperator<Tuple2<String, Integer>> words = source.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                //切单词
                String[] split = value.split("\\s+");
                for (String word:split) {
                    out.collect(Tuple2.of(word,1));
                }
            }
        });

        KeyedStream<Tuple2<String, Integer>, String> keyedStream = words.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> value) throws Exception {
                return value.f0;
            }
        });

        SingleOutputStreamOperator<Tuple2<String, Integer>> resultStream = keyedStream.sum("f1");

        // TODO 4. sink算子 输出结果
        resultStream.print("result>>>>>");

        // TODO 5.提交程序
        env.execute("WC");

    }

}
