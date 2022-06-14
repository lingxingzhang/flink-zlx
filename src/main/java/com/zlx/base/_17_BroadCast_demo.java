package com.zlx.base;

import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
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
 *   source2流中对应的是维表数据 source1关联source2的数据 进行输出
 *
 */
public class _17_BroadCast_demo {

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

        MapStateDescriptor<Integer, Tuple2<Integer, String>> stateDesc =
                new MapStateDescriptor<>("source2StateDesc", TypeInformation.of(Integer.class), TypeInformation.of(new TypeHint<Tuple2<Integer, String>>() {
        }));

        BroadcastStream<Tuple3<Integer, Integer, String>> broadcast = map2.broadcast(stateDesc);

        SingleOutputStreamOperator<String> result = map1
                .connect(broadcast)
                .process(new BroadcastProcessFunction<Tuple2<Integer, String>, Tuple3<Integer, Integer, String>, String>() {

                    // 我们规则流中的数据不会很大 这样使用会不会更加好点 只要processElement中不修改 即可？？？
                    BroadcastState<Integer, Tuple2<Integer, String>> state;

                    /**
                     * 每来一条 日志数据 调用该方法
                     * @param value
                     * @param ctx
                     * @param out
                     * @throws Exception
                     */
                    @Override
                    public void processElement(Tuple2<Integer, String> value, ReadOnlyContext ctx, Collector<String> out) throws Exception {

                        // 从flink框架中获取对应的 state的数据
                        // ReadOnlyBroadcastState<Integer, Tuple2<Integer, String>> state = ctx.getBroadcastState(stateDesc);
                        // 从成员变量中获取对应的状态数据
                        if( null != state){
                            Tuple2<Integer, String> userInfo = state.get(value.f0);
                            out.collect(value.f0+"--"+value.f1+"--"+(userInfo==null?null:userInfo.f0)+"--"+(userInfo==null?null:userInfo.f1));
                        }else{
                            out.collect(value.f0+"--"+value.f1+"--"+null+"--"+null);
                        }
                    }
                    @Override
                    public void processBroadcastElement(Tuple3<Integer, Integer, String> value, Context ctx, Collector<String> out) throws Exception {

                        //获取flink管理的状态数据
                        // 第一种方式 从flink管理的框架中获取 状态
                       // BroadcastState<Integer, Tuple2<Integer, String>> state = ctx.getBroadcastState(stateDesc);

                        // 第二种 我们可以复制给成员变量
                        state = ctx.getBroadcastState(stateDesc);

                        // 将最新的数据添加到state中
                        state.put(value.f0,Tuple2.of(value.f1,value.f2));

                    }
                });


        result.print("BroadCast实现维表数据关联>>>>");

        /**
         * 结果如下：
             Cogroup实现left join>>>>:2> 1-zs-1-18-bj
             Cogroup实现left join>>>>:2> 3-zw-null-null-null
             Cogroup实现left join>>>>:2> 2-lisi-2-20-sh
         */

        env.execute("_16_CoGroup_demo");

    }
}
