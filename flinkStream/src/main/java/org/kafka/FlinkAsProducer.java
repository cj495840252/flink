package org.kafka;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.ArrayList;
import java.util.Properties;

public class FlinkAsProducer {
    public static void main(String[] args) throws Exception {
        // 1.获取环境
        StreamExecutionEnvironment flink = StreamExecutionEnvironment.getExecutionEnvironment();
        flink.setParallelism(3);

        // 2.准备数据源
        ArrayList<String> list = new ArrayList<>();
        list.add("message1");
        list.add("message2");
        DataStreamSource<String> stream = flink.fromCollection(list);

        // 2.1 创建kafka生产者
        //方法一:新版本
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"node1:9092,node2:9092,node3:9092");
        KafkaSink<String> producer =  KafkaSink.<String>builder()
                        .setBootstrapServers("node1:9092,node2:9092,node3:9092")
                        .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("test")
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build()
                ).setDeliverGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .setKafkaProducerConfig(properties).build();

        // 方法二：旧版本
        FlinkKafkaProducer kafkaProducer = new FlinkKafkaProducer("test",new SimpleStringSchema(),properties);
        // 3.添加数据源,addsink保存数据到其他数据库中
        //stream.addSink(kafkaProducer);
        stream.sinkTo(producer);
        // 4.执行代码
        flink.execute();
    }
}
