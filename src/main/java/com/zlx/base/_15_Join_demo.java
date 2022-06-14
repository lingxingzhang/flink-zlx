package com.zlx.base;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

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
 *    使用join操作来关联两个流并开窗
 *    join：inner join
 *    flink 框架给我们返回可以匹配上的数据，我们只需要写我们的业务逻辑开发即可
 *    Tuple2<Integer, String> first, Tuple3<Integer, Integer, String> second
 *
 */
public class _15_Join_demo {

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

        DataStream<String> result = map1
                .join(map2)
                .where(f -> f.f0)
                .equalTo(f -> f.f0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(3)))
                .apply(new JoinFunction<Tuple2<Integer, String>, Tuple3<Integer, Integer, String>, String>() {
                    @Override
                    public String join(Tuple2<Integer, String> first, Tuple3<Integer, Integer, String> second) throws Exception {
                        return "join结果【"+first.f0+"-"+first.f1+"-"+second.f0+"-"+second.f1+"-"+second.f2+"】";
                    }
                });

        result.print("结果>>>>");

        env.execute("_15_Join_demo");

    }
}
