package com.zlx.base;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

/**
 * flink 读取 kafka的数据
 */
public class _05_Source_kafka {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        // 初始化一个kafka souce
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setTopics("tp01") // 设置topic主题
                .setGroupId("gp01") //设置组ID
                .setBootstrapServers("node:9092")
                // setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.LATEST)) 消费起始位置选择之前提交的偏移量（如果没有，则重置为latest）
                // .setStartingOffsets(OffsetsInitializer.latest()) 最新的
                // .setStartingOffsets(OffsetsInitializer.earliest()) 最早的
                // .setStartingOffsets(OffsetsInitializer.offsets(Map< TopicPartition,Long >)) 之前具体的偏移量进行消费 每个分区对应的偏移量
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.LATEST))
                .setValueOnlyDeserializer(new SimpleStringSchema())
                // 默认是关闭的
                // 开启了kafka的自动提交偏移量机制 会把偏移量提交到 kafka的 consumer_offsets中
                // 就算算子开启了自动提交偏移量机制，kafkaSource依然不依赖自动提交的偏移量（优先从flink自动管理的状态中获取对应的偏移量 如果获取不到就会用自动提交的偏移量）
                //将本source算子设置为 Bounded属性（有界流），将来改source去读取数据的时候，读到指定的位置，就停止读取并退出程序
                .setProperty("auto.offset.commit", "ture")
                .build();

        // 常用于补数或者读取某一段历史数据
                // 可以设置到读取到的最大偏移量
                //.setBounded(OffsetsInitializer.committedOffsets())
                // 把本source的算子设置为 unBounded属性（无界流）和上面的区别是程序不退出
                //.setUnbounded(OffsetsInitializer.latest())

        env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(),"kafka-source");

        env.execute("_05_Source_kafka");

    }

}
