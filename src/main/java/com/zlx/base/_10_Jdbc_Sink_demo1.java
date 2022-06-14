package com.zlx.base;

import com.alibaba.fastjson.JSON;
import com.zlx.base.bean.EventLog;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * 不能保证精准一次
 */
public class _10_Jdbc_Sink_demo1 {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //开启checkpoint
        env.enableCheckpointing(10000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage("file:////Volumes/D/tmp/flink/ckpt");
        env.setParallelism(2);

        DataStreamSource<EventLog> result = env.addSource(new MySourceFunction());

        SinkFunction<EventLog> jdbcSink = JdbcSink.sink(
                "insert into t_eventlog values(?,?,?,?,?) on duplicate key update sessionId=?,eventId=?,ts=?,eveenntInfo=?",
                new JdbcStatementBuilder<EventLog>() {
                    @Override
                    public void accept(PreparedStatement stmt, EventLog eventLog) throws SQLException {
                        stmt.setLong(1, eventLog.getGuid());
                        stmt.setString(2, eventLog.getSessionId());
                        stmt.setString(3, eventLog.getEventId());
                        stmt.setLong(4, eventLog.getTimeStamp());
                        stmt.setString(5, JSON.toJSONString(eventLog.getEventInfo()));
                    }
                },
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUsername("root")
                        .withPassword("123456")
                        .withUrl("jdbc:mysql://node01:3306/abc")
                        .build()
        );

        result.addSink(jdbcSink);

        env.execute("_10_Jdbc_Sink_demo");


    }
}
