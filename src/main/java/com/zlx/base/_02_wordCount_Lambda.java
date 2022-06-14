package com.zlx.base;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;


public class _02_wordCount_Lambda {

    public static void main(String[] args) throws Exception {

        // 创建环境变量
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> source = env.readTextFile("data/wc/input/");

        // lambda 表达式，本质上就是 单方法接口 的方法 实现的简洁语法表达
    /*    从map算子接受的mapFunction的接口实现来看，他就是一个单接口
        source.map(new MapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
                return null;
            }
        })*/
        // lambda 表达式 怎么写 看你要实现的那个接口 接受什么参数，返回什么结果
        // 然后就按lambda语法来表达 (参数1，参数2....) -> {函数体}
        // 第一种写法
        //source.map( (value) -> {return value.toUpperCase();} );

        // 由于上面的lambda表达式 参数列表只有一个参数，所以可以进行简写
        // 第二种写法
        //source.map(value -> value.toUpperCase());

        // 由于上面的lambda表达式 函数体只有一行代码，且其中的方法调用没有参数传递，则可以将方法的调用转成"方法引用"
        SingleOutputStreamOperator<String> upperCaseed = source.map(String::toUpperCase);

        //然后划分单词 并转成（单词,1）并压平

      /*  upperCaseed.flatMap(new FlatMapFunction<String, Tuple2<String,Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {

            }
        })*/
        // 从上面的代码 我们可以看出 flatMap 也是一个单方法的接口 所以可以用lambda的方式


        // 泛型只是在编译的时候 进行约束
        //运行的时候 丢弃了 Tuple<String,Integer>
        // TypeHint 泛型提示
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndone =
                upperCaseed.flatMap((String value, Collector<Tuple2<String,Integer>> out) -> {
                    String[] split = value.split("\\s+");
                    for (String word : split) {
                        out.collect(Tuple2.of(word, 1));
                    }
                })
                //.returns(new TypeHint<Tuple2<String,Integer>>() {}) // 通过TypeHint生成TypeInformation
                //.returns(TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {}))
                .returns(Types.TUPLE(Types.STRING,Types.INT)) // 用工具类生成TypeInformation
                ;


     /*   wordAndone.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {\

            @Override
            public String getKey(Tuple2<String, Integer> value) throws Exception {
                return null;
            }
        })
        */

        // 从上面的代码 我们可以看出 keyBy 也是一个单方法的接口 所以可以用lambda的方式


        KeyedStream<Tuple2<String, Integer>, String> keyedStream = wordAndone.keyBy(value -> value.f0);

        SingleOutputStreamOperator<Tuple2<String, Integer>> result = keyedStream.sum("f1");

        result.print();

        env.execute("lambda WC");

    }

}
