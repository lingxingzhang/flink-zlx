package com.zlx.base;

import com.zlx.base.MySourceFunction;
import com.zlx.base.avro.schema.AvroEventLog;
import com.zlx.base.bean.EventLog;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.parquet.ParquetWriterFactory;
import org.apache.flink.formats.parquet.avro.ParquetAvroWriters;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy;

import java.util.HashMap;
import java.util.Map;

/**
 * org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink}
 *
 * StreamingFileSink 必须开启checkpoinnt!!!
 * 从源码可以看出来，写入过程中三个状态
 *  Part files can be in one of three states: {@code in-progress}, {@code pending} or {@code
 *  * finished}
 *  in-progress:正在写入中
 *  pending：挂起
 *  finished：已完成
 */
public class _09_StreamFlileSink_Demo3 {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //开启checkpoint
        env.enableCheckpointing(10000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage("file:////Volumes/D/tmp/flink/ckpt");

        env.setParallelism(5);

        DataStreamSource<EventLog> result = env.addSource(new MySourceFunction());

        /**
         * 1.编写avsc文本文件（json）来描述数据格式
         * 2.添加maven插件生成对应的javabean
         * 3.利用生成的javabean【自带schema信息】
         * 4. 然后构造一个 writerFactory
         */

        // 自带schema信息
//        AvroEventLog avroEventLog = new AvroEventLog();
//        Schema schema = avroEventLog.getSchema();

        ParquetWriterFactory<EventLog> writerFactory = ParquetAvroWriters.forReflectRecord(EventLog.class);

        FileSink<EventLog> fileSink = FileSink
                .forBulkFormat(new Path("/Volumes/D/tmp/flink/data_demo3"), writerFactory)
                .withBucketAssigner(new DateTimeBucketAssigner<>("yyyy-MM-dd--HH")) // 分桶的策略【文件夹下面的子文件夹】
                .withBucketCheckInterval(5) //检查分桶的间隔时间
                .withRollingPolicy(OnCheckpointRollingPolicy.build()) // 滚动的策略，只能有一种 因为parquet格式 需要checkpoint的时候生成特定的格式
                .withOutputFileConfig(OutputFileConfig.builder().withPartPrefix("com_zlx").withPartSuffix(".parquet").build())
                .build();

        // 将source进行转换为 AvroEventLog
        result.sinkTo(fileSink);

        env.execute("_09_StreamFlileSink_Demo3");

    }
}
