package com.zlx.base;

import com.alibaba.fastjson.JSON;
import com.mysql.cj.jdbc.MysqlXAConnection;
import com.mysql.cj.jdbc.MysqlXADataSource;
import com.zlx.base.bean.EventLog;
import org.apache.flink.connector.jdbc.*;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.util.function.SerializableSupplier;

import javax.sql.XADataSource;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * 保证精准一次
 */
public class _10_Jdbc_Sink_demo2 {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //开启checkpoint
        env.enableCheckpointing(10000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage("file:////Volumes/D/tmp/flink/ckpt");
        env.setParallelism(2);

        DataStreamSource<EventLog> result = env.addSource(new MySourceFunction());

        SinkFunction<EventLog> sinkFunction = JdbcSink.exactlyOnceSink(
                "insert into t_eventlog values(?,?,?,?,?) on duplicate key update guid=?,session_id=?",
                new JdbcStatementBuilder<EventLog>() {
                    @Override
                    public void accept(PreparedStatement stmt, EventLog eventLog) throws SQLException {
                        stmt.setLong(1, eventLog.getGuid());
                        stmt.setString(2, eventLog.getSessionId());
                        stmt.setString(3, eventLog.getEventId());
                        stmt.setLong(4, eventLog.getTimeStamp());
                        stmt.setString(5, JSON.toJSONString(eventLog.getEventInfo()));

                        stmt.setLong(6, eventLog.getGuid());
                        stmt.setString(7, eventLog.getSessionId());
                    }
                },
                JdbcExecutionOptions.builder().withBatchSize(5).withMaxRetries(3).build(),
                JdbcExactlyOnceOptions.builder().withTransactionPerConnection(true).build(),//mysql 一个连接只能同时存在一个事务，所以必须设置
                new SerializableSupplier<XADataSource>() {
                    @Override
                    public XADataSource get() {
                        MysqlXADataSource xaDataSource = new MysqlXADataSource();
                        xaDataSource.setUser("root");
                        xaDataSource.setUrl("jdbc:mysql//node01:3306/abc");
                        xaDataSource.setPassword("123456");
                        return xaDataSource;
                    }
                }
        );

        result.addSink(sinkFunction);

        env.execute("_10_Jdbc_Sink_demo");


    }
}
