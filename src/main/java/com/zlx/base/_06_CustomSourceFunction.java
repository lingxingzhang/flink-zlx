package com.zlx.base;

import com.zlx.base.bean.EventLog;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.HashMap;

/**
 * 自定义source的连接
 */
public class _06_CustomSourceFunction {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<EventLog> source = env.addSource(new MySourceFunction());

        source.print();

        env.execute("_06_CustomSourceFunction");
    }


}

class MySourceFunction implements SourceFunction<EventLog>{

    volatile boolean flag = true;

    @Override
    public void run(SourceContext<EventLog> ctx) throws Exception {

        EventLog eventLog = new EventLog();
        String [] events = {"A","B","C","D","E"};
        HashMap<String,String>  eventInfoMap = new HashMap<>();

        while (flag){

            eventLog.setGuid(RandomUtils.nextLong(1,1000));
            eventLog.setSessionId(RandomStringUtils.randomAlphanumeric(10));
            eventLog.setTimeStamp(System.currentTimeMillis());
            eventLog.setEventId(events[RandomUtils.nextInt(0,events.length)]);

            eventInfoMap.put(RandomStringUtils.randomAlphanumeric(2),RandomStringUtils.randomAlphanumeric(2));
            eventLog.setEventInfo(eventInfoMap);

            ctx.collect(eventLog);

            eventInfoMap.clear();
            Thread.sleep(RandomUtils.nextLong(10,100));

        }

    }

    @Override
    public void cancel() {

    }
}



/**
 * 并行的source 实现和上面的一样
 */
class MySourceFunction2 implements ParallelSourceFunction<EventLog>{
    @Override
    public void run(SourceContext<EventLog> ctx) throws Exception {

    }

    @Override
    public void cancel() {

    }
}