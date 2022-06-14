package com.zlx.base;

import com.zlx.base.bean.EventLog;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Map;

/**
 * 从 writeAsCsv 源码提示 可以看出来 该方法应该过时 可以使用 StreamingFileSink
 * 并且StreamingFileSink方法有如下的特征【可以在源码中看到如下的解释】
 *   * Sink that emits its input elements to {@link FileSystem} files within buckets. This is integrated
 *   * with the checkpointing mechanism to provide exactly once semantics.
 */
public class _08_SinkOperator_Demos {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        DataStreamSource<EventLog> source = env.addSource(new MySourceFunction());

        //source.writeAsText("/Volumes/D/tmp/flink/data");

        source
                .map( bean -> Tuple4.of(bean.getGuid(),bean.getSessionId(),bean.getEventId(),bean.getEventInfo()))
                .returns(new TypeHint<Tuple4<Long, String, String, Map<String, String>>>() {})
                .writeAsCsv("/Volumes/D/tmp/flink/data2", FileSystem.WriteMode.OVERWRITE);

        env.execute("_08_SinkOperator_Demos");

    }

}
