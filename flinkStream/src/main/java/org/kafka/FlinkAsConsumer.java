package org.kafka;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;


import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


public class FlinkAsConsumer {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        List<String> list = new ArrayList<>();
        list.add("first");
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setTopics(list)
                .setGroupId("test")
                .setBootstrapServers("node1:9092,node2:9092")
                // 读取数据的起始位置
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.LATEST))//最新
                //.setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.LATEST) //最早
                //.setStartingOffsets(OffsetsInitializer.offsets(Map.of(new TopicPartition("主题1",1),100L)))
                .setValueOnlyDeserializer(new DeserializationSchema<String>() {
                    @Override
                    public String deserialize(byte[] bytes) throws IOException {
                        // bytes：就是传过来的消息的value，二进制，转字符串返回即可
                        return new String(bytes);
                    }

                    @Override
                    public boolean isEndOfStream(String s) {
                        return false;
                    }

                    @Override
                    public TypeInformation<String> getProducedType() {
                        return TypeInformation.of(String.class);
                    }
                })//反序列化new SimpleStringSchema(),上述反序列化javabean时需要自己写
                // 默认无界流，设置有界流，设置了读到该位置则停止，且退出程序，常用于补数或重跑
//                .setBounded(OffsetsInitializer.committedOffsets())
                // 设置成无界流（假无界），但是并不会一直读下去，到达指定位置停止，但是程序不退出
//                .setUnbounded(OffsetsInitializer.latest())
                // flink有一个状态管理机制的，它把kafka作为了算子，算子状态记录了读取kafka的topic的分区的偏移量
                // flink优先按自己记录的来读，再按kafka的consumer读取
                .setProperty("auto.offset.commit", "true")//设置kafka的自动提交
                .build();
        //添加Source算子的实现接口,
        DataStreamSource<String> streamSource = env.fromSource(kafkaSource,
                WatermarkStrategy.noWatermarks(), "随便的name", Types.STRING);
        streamSource.print();
        //env.addSource();//添加FunctionSource算子的实现接口
        env.execute();
    }
}
