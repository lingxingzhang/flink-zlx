package com.zlx.base;

import org.apache.flink.api.common.functions.CoGroupFunction;
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
import org.apache.flink.util.Collector;

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
 *          cogroup 是join的底层调用的算子 更加灵活 模拟一个left join的场景
 *
 *          flink框架帮我们封装好的结构如下：
 *              Iterable<Tuple2<Integer, String>> first, Iterable<Tuple3<Integer, Integer
 *          flink把左表和右表能关联上的数据都全部放到各自的Iterable中
 *
 */
public class _16_CoGroup_demo {

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


        map1.print("map1>>>>");

        // 数据格式： id,age,city
        DataStreamSource<String> source2 = env.socketTextStream("localhost", 9999);

        SingleOutputStreamOperator<Tuple3<Integer,Integer, String>> map2 = source2.map(
                line -> {
                    String[] split = line.split(",");
                    return Tuple3.of(Integer.parseInt(split[0]), Integer.parseInt(split[1]),split[2]);
                }).returns(new TypeHint<Tuple3<Integer, Integer,String>>() {
        });

        map2.print("map2>>>>");

        DataStream<String> result = map1.coGroup(map2)
                .where(f -> f.f0)
                .equalTo(f -> f.f0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(30)))
                .apply(new CoGroupFunction<Tuple2<Integer, String>, Tuple3<Integer, Integer, String>, String>() {
                    @Override
                    public void coGroup(Iterable<Tuple2<Integer, String>> first, Iterable<Tuple3<Integer, Integer, String>> second, Collector<String> out) throws Exception {
                        // 模拟left join的场景
                        for (Tuple2<Integer, String> t1 : first) {
                            boolean flag = false;
                            for (Tuple3<Integer, Integer, String> t2 : second) {
                                out.collect(t1.f0+"-"+t1.f1+"-"+t2.f0+"-"+t2.f1+"-"+t2.f2);
                                flag = true;
                            }
                            if(!flag){ //右表没有数据
                                out.collect(t1.f0+"-"+t1.f1+"-"+null+"-"+null+"-"+null);
                            }
                        }
                    }
                });

        result.print("Cogroup实现left join>>>>");

        /**
         * 结果如下：
             Cogroup实现left join>>>>:2> 1-zs-1-18-bj
             Cogroup实现left join>>>>:2> 3-zw-null-null-null
             Cogroup实现left join>>>>:2> 2-lisi-2-20-sh
         */

        env.execute("_16_CoGroup_demo");

    }
}
