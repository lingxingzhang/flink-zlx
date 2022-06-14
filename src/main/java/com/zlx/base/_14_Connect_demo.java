package com.zlx.base;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

/**
 * 需求：有两个流
 *  source1
 *      数据结构：id,name
 *      数据：
 *          1,zs
 *          2,lisi
 *          3,zw
 *  source2
 *       数据结构：id,age,city
 *          1,18,bj
 *          2,20,sh
 *          4,33,gz
 *
 *     使用connect 功能比较弱 就是将两个流合并起来处理而已
 */
public class _14_Connect_demo {

    public static void main(String[] args) throws Exception {

        Configuration config = new Configuration();
        config.setInteger("rest.prot",8822);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(config);

        //开启checkpoint
        env.enableCheckpointing(10000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage("file:////Volumes/D/tmp/flink/ckpt");
        env.setParallelism(2);

        //数据格式是 Id,name
        DataStreamSource<String> source1 = env.socketTextStream("localhost", 9998);

        SingleOutputStreamOperator<Tuple2<Integer, String>> map1 = source1.map(
                line -> {
                    String[] split = line.split(",");
                    return Tuple2.of(Integer.parseInt(split[0]), split[1]);
                }).returns(new TypeHint<Tuple2<Integer, String>>() {
        });

        // 数据格式： id,age,city
        DataStreamSource<String> source2 = env.socketTextStream("localhost", 9999);

        SingleOutputStreamOperator<Tuple3<Integer,Integer, String>> map2 = source2.map(
                line -> {
                    String[] split = line.split(",");
                    return Tuple3.of(Integer.parseInt(split[0]), Integer.parseInt(split[1]),split[2]);
                }).returns(new TypeHint<Tuple3<Integer, Integer,String>>() {
        });

        SingleOutputStreamOperator<String> result = map1.connect(map2).map(new CoMapFunction<Tuple2<Integer, String>, Tuple3<Integer, Integer, String>, String>() {
            @Override
            public String map1(Tuple2<Integer, String> value) throws Exception {
                return "source1[" + value.f0 + "--" + value.f1 + "]";
            }

            @Override
            public String map2(Tuple3<Integer, Integer, String> value) throws Exception {
                return "source2[" + value.f0 + "--" + value.f1 + "--" + value.f2 + "]";
            }
        });

        // union 操作算子需要两个的类型相同 就不写了

        result.print("connect的结果输出：");

        env.execute("_14_Join_demo");

    }
}
