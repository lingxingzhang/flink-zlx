package com.zlx.base;

import com.zlx.base.bean.EventLog;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.avro.typeutils.GenericRecordAvroTypeInfo;
import org.apache.flink.formats.parquet.ParquetWriterFactory;
import org.apache.flink.formats.parquet.avro.ParquetAvroWriters;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy;

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
public class _09_StreamFlileSink_Demo1 {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //开启checkpoint
        env.enableCheckpointing(10000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage("file:////Volumes/D/tmp/flink/ckpt");

        env.setParallelism(5);

        DataStreamSource<EventLog> source = env.addSource(new MySourceFunction());

        /**
         * BulkWriter.Factory 我们也不会写 我们可以看一下flink给我们提供的
         * 可以通过工具来
         *
         *     private Long guid;
         *     private String sessionId;
         *     private String eventId;
         *     private Long timeStamp;
         *     private Map<String,String> eventInfo;
         *
         */
        Schema schema = SchemaBuilder.builder()
                .record("DataRecord") //
                .namespace("com.zlx.base.avro.schema")
                .doc("用户行为测试数据")
                .fields()
                .requiredLong("guid")
                .requiredString("sessionId")
                .requiredString("eventId")
                .requiredLong("timeStamp")
                .name("eventInfo")
                    .type()
                    .map()
                    .values()
                    .type("string")
                    .noDefault()
                .endRecord();

        ParquetWriterFactory<GenericRecord> writerFactory = ParquetAvroWriters.forGenericRecord(schema);

        FileSink<GenericRecord> fileSink = FileSink
                .forBulkFormat(new Path("/Volumes/D/tmp/flink/data_demo1"), writerFactory)
                .withBucketAssigner(new DateTimeBucketAssigner<>("yyyy-MM-dd--HH")) // 分桶的策略【文件夹下面的子文件夹】
                .withBucketCheckInterval(5) //检查分桶的间隔时间
                .withRollingPolicy(OnCheckpointRollingPolicy.build()) // 滚动的策略，只能有一种 因为parquet格式 需要checkpoint的时候生成特定的格式
                .withOutputFileConfig(OutputFileConfig.builder().withPartPrefix("com_zlx").withPartSuffix(".parquet").build())
                .build();

        // 将数据进行转换为GenericRecord 然后写入
        SingleOutputStreamOperator<GenericRecord> result = source.map(
                (MapFunction<EventLog, GenericRecord>) bean -> {
            GenericData.Record record = new GenericData.Record(schema);
            record.put("guid", bean.getGuid());
            record.put("sessionId", bean.getSessionId());
            record.put("eventId", bean.getEventId());
            record.put("timeStamp", bean.getTimeStamp());
            record.put("eventInfo", bean.getEventInfo());
            return record;
        }).returns(new GenericRecordAvroTypeInfo(schema)); //由于avro相关的类 对象要用avro的序列化，所以需要显式的指定 最好配合lambda表示配合使用

        result.sinkTo(fileSink);

        env.execute("_09_StreamFlileSink_Demo1");

    }
}
