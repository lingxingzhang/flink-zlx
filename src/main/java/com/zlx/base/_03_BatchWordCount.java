package com.zlx.base;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class _03_BatchWordCount {

    public static void main(String[] args) throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSource<String> source = env.readTextFile("data/wc.input");

        source
                .flatMap(new MyFlatFunction())
                .groupBy(0)
                .sum(1)
                .print();


    }
}

class MyFlatFunction implements FlatMapFunction<String , Tuple2<String,Integer>>{
    @Override
    public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
        String[] split = value.split("\\s+");
        for (String word: split ) {
            out.collect(Tuple2.of(word,1));
        }
    }
}