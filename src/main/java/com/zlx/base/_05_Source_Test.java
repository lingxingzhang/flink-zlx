package com.zlx.base;

import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;

import java.util.Arrays;

/**
 * 常用的测试source
 */
public class _05_Source_Test {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> source = env.fromElements("hello", "spark", "flink", "hello");

        //source.map(String::toUpperCase).print();

        // 单并行度的算子 并发只能1 如果并行度大于1 则会报错
        DataStreamSource<String> source1 = env.fromCollection(Arrays.asList("a","b"));
        //source1.print();

        DataStreamSource<String> source2 = env.readTextFile("data/wc/input/wc.txt","utf-8");
        source2.print();

        //  FileProcessingMode.PROCESS_CONTINUOUSLY 监听文件的变化 如果有变化 会全量在发送一次数据 不能发送增量数据
        //   FileProcessingMode.PROCESS_ONCE, 表示只读一次文件
        DataStreamSource<String> source3 =
                env.readFile(new TextInputFormat(null),
                    "data/wc/input/wc.txt",
                    FileProcessingMode.PROCESS_CONTINUOUSLY,
                    1000);
        source3.print();

        env.execute("_05_Source_Test");
    }
}
