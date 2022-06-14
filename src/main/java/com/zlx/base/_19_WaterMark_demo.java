package com.zlx.base;

import com.zlx.base.bean.EventBean;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 *
 * 单平行度 waterMark的讲解
 *
 * 时间格式如下
 *  1,e01,1000,pg01
 *
 *  map1[生成 watermark，定期200ms]  -传递-> process1算子 []
 *
 *  从头分析：
 *      本程序启动以后，不发送任何数据
 *      因为程序中的mapdatastream调用注册watermark的算子，所以，会每个【200ms】BoundedOutOfOrdernessWatermarks的这个onPeriodicEmit方法，
 *      可以打个断点观察【此时的watermark=Long.MIN_VALUE-1】。
 *      watermark= Long.MIN_VALUE -1 [200ms] 调用一次
 *
 *     在  BoundedOutOfOrdernessWatermarks的onEvent方法打断点
 *      当用户输入了
 *      1,e01,1000,pg01
 *      会看到方法的调用栈 进入 TimestampsAndWatermarksOperator 的  processElement 方法
 *
 *      看到如下最核心的两个代码
 *       output.collect(element); // 将数据原封不动的发到下游的算子，然后在走下面的方法
 *       watermarkGenerator.onEvent(event, newTimestamp, wmOutput);
 *
 *
 *
 *
 *
 *
 */
public class _19_WaterMark_demo {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> source = env.socketTextStream("localhost", 9988);


        // 从源头sources算子生成watermark
        // 不生成Watermark，禁用了 "事件时间"推进机制
        //watermarkStrategy = WatermarkStrategy.noWatermarks();
        // 紧跟最大时间（是下面的策略的特例） Duration.ofMillis(0)
        // watermarkStrategy = WatermarkStrategy.forMonotonousTimestamps();
        // 允许最大的乱序
        WatermarkStrategy<String> watermarkStrategy = WatermarkStrategy
                .<String>forBoundedOutOfOrderness(Duration.ofMillis(2000))
                .withTimestampAssigner(
                        (element, recordTimestamp) -> Long.parseLong(element.split(",")[2])
                );

        // 给source分配 水印的生成策略
        /*source.assignTimestampsAndWatermarks(watermarkStrategy);*/

        /**
         * 从中间算子生成watermark
         */

        SingleOutputStreamOperator<EventBean> map1 = source.map(s -> {
            String[] split = s.split(",");
            return new EventBean(Long.parseLong(split[0]), split[1], Long.parseLong(split[2]), split[3]);
        }).returns(EventBean.class)
               .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<EventBean>forBoundedOutOfOrderness(Duration.ofMillis(0))
                                .withTimestampAssigner((element, recordTimestamp) -> element.getTimeStamp())
           );


        SingleOutputStreamOperator<EventBean> process1 = map1.process(new ProcessFunction<EventBean, EventBean>() {
            @Override
            public void processElement(EventBean value, Context ctx, Collector<EventBean> out) throws Exception {

                long currentWatermark = ctx.timerService().currentWatermark();
                long currentProcessingTime = ctx.timerService().currentProcessingTime();
                System.out.println("process1>本次收到的数据="+value);
                System.out.println("process1>currentWatermark="+currentWatermark);
                System.out.println("process1>currentProcessingTime="+currentProcessingTime);
                out.collect(value);
            }
        });

       SingleOutputStreamOperator<EventBean> process2 =  process1.process(new ProcessFunction<EventBean, EventBean>() {
            @Override
            public void processElement(EventBean value, Context ctx, Collector<EventBean> out) throws Exception {
                long currentWatermark = ctx.timerService().currentWatermark();
                long currentProcessingTime = ctx.timerService().currentProcessingTime();
                System.out.println("process2------->currentWatermark="+currentWatermark);
                out.collect(value);
            }
        });

        process2.print("process2>>>");

        env.execute("_19_WaterMark_demo");

    }

}
