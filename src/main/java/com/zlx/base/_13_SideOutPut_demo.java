package com.zlx.base;

import com.alibaba.fastjson.JSON;
import com.zlx.base.bean.EventLog;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * 使用测流进行 切分
 */
public class _13_SideOutPut_demo {

    public static void main(String[] args) throws Exception {

        Configuration config = new Configuration();
        config.setInteger("rest.prot",8822);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(config);

        //开启checkpoint
        env.enableCheckpointing(10000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage("file:////Volumes/D/tmp/flink/ckpt");
        env.setParallelism(2);

        DataStreamSource<EventLog> source = env.addSource(new MySourceFunction());

         OutputTag<EventLog>  outputTagA  =  new OutputTag<EventLog>("outputTagA", TypeInformation.of(EventLog.class));

        OutputTag<String>  outputTagB  =  new OutputTag<String>("outputTagB", TypeInformation.of(String.class));

        // 使用process进行流的切分
        SingleOutputStreamOperator<EventLog> processed = source.process(new ProcessFunction<EventLog, EventLog>() {
            @Override
            public void processElement(EventLog value, Context ctx, Collector<EventLog> out) throws Exception {
                String eventId = value.getEventId();
                if ("A".equals(eventId)) { //输出到侧输出流
                    ctx.output(outputTagA, value);
                } else if ("B".equals(eventId)) {
                    ctx.output(outputTagB, JSON.toJSONString(value));
                }
                out.collect(value);
            }
        });

        processed.print("主流》》》》");

        DataStream<EventLog> outputA = processed.getSideOutput(outputTagA);
        DataStream<String> outputB = processed.getSideOutput(outputTagB);

        outputA.print("outputA》》》》");
        outputA.print("outputB》》》》");


        env.execute("_13_SideOutPut_demo");

    }

}
