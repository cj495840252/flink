package org.flink.demos.sink_function;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.flink.demos.source_function.init.EventLog;
import org.flink.demos.source_function.init.MySourceFunction;

public class KafkaSink_Producer {
    public static void main(String[] args) throws Exception {
        String path = "file:///F:/JavaProject/flink-test/mode1/src/outfile/";
        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port",8081);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        env.setParallelism(1);
        env.enableCheckpointing(500, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage(path+"checkpoint");

        KafkaSink<String> kafkaSink = KafkaSink.<String>builder().setBootstrapServers("node1:9092,node2:9092,node3:9092")
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.builder()
                                .setTopic("test")
                                .setValueSerializationSchema(new SimpleStringSchema()).build())
                .setDeliverGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .setTransactionalIdPrefix("kafka-flink--")
                .build();

        DataStreamSource<EventLog> source = env.addSource(new MySourceFunction());
        source.map(EventLog::toString).disableChaining().sinkTo(kafkaSink);
                //.disableChaining();//让他俩不绑定在一起
        env.execute();
    }
}
