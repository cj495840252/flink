package org.flink.demos.sink_function.stramfilesink;

import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;

import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.parquet.ParquetWriterFactory;
import org.apache.flink.formats.parquet.avro.ParquetAvroWriters;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy;

import org.flink.demos.avroschema.EventBean;
import org.flink.demos.source_function.init.EventLog;
import org.flink.demos.source_function.init.MySourceFunction;

import java.util.HashMap;


public class Demo2 {
    public static void main(String[] args) throws Exception {
//        String path = "F:\\JavaProject\\flink-test\\mode1\\src\\outfile\\";

        String path = "file:///F:/JavaProject/flink-test/mode1/src/outfile/";

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(2000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage(path+"checkpoint");
        env.setParallelism(1);
        DataStreamSource<EventLog> source = env.addSource(new MySourceFunction());
        // StreamingFileSink方法
        // 输出为行格式
        FileSink<String> build = FileSink.forRowFormat(new Path(path+"data"), new SimpleStringEncoder<String>("utf8"))
                //文件滚动的策略
                .withRollingPolicy(DefaultRollingPolicy.builder()
                        // 10s滚动一次
                        .withRolloverInterval(10000)
                        // 多少大小滚动
                        .withMaxPartSize(1024 * 1024).build())
                // 分桶的策略，按时间,DateTimeBucketAssigner<>泛型要求一条数据的格式，写出javabean，则要求该bean
                .withBucketAssigner(new DateTimeBucketAssigner<String>())
                // 多久检查一次该桶满了没。是不是要创建下一个桶了
                .withBucketCheckInterval(5)
                .withOutputFileConfig(
                        // 输出文件的相关配置，文件名和类型
                        OutputFileConfig.builder()
                                .withPartPrefix("doitedu")
                                .withPartSuffix(".txt")
                                .build())
                .build();
        DataStream<String> map1 = source.map(JSON::toJSONString);
        //map1.sinkTo(build);


        // 输出成列格式
        // 根据调用当方法即传入的信息，获取avro模式和schema，生成ParquetWriter。因为parquet自带schema
        // 方法一
        // ParquetAvroWriters.forGenericRecord(new Schema());//自己生成太麻烦，不建议
        // 方法二
        ParquetWriterFactory<EventBean> schema = ParquetAvroWriters.forSpecificRecord(EventBean.class);//需要avro文件，读取反射成自己的schema
        FileSink<EventBean> sink2 = FileSink.forBulkFormat(new Path(path), schema)
                //文件滚动的策略,只有一种滚动模式，只有发生了checkpoint时才滚动
                .withRollingPolicy(OnCheckpointRollingPolicy.build())
                // 分桶的策略，按时间
                .withBucketAssigner(new DateTimeBucketAssigner<EventBean>())
                // 多久检查一次该桶满了没。是不是要创建下一个桶了
                .withBucketCheckInterval(5)
                .withOutputFileConfig(
                        // 输出文件的相关配置，文件名和类型
                        OutputFileConfig.builder()
                                .withPartPrefix("location")
                                .withPartSuffix(".parquet")
                                .build())
                .build();
        DataStream<EventBean> map2 = source.map(eventLog -> {
            HashMap<CharSequence, CharSequence> hashMap = new HashMap<>(eventLog.getEventInfo());
            return new EventBean(eventLog.getGuid(), eventLog.getSessionId(),
                    eventLog.getEventId(), eventLog.getTimeStamp(), hashMap);
        }).returns(EventBean.class);
//         map2.sinkTo(sink2);
        // 方法三
        // 传入普通的一个javaBean即可
        ParquetWriterFactory<EventLog> factory = ParquetAvroWriters.forReflectRecord(EventLog.class);
        FileSink<EventLog> sink3 = FileSink.forBulkFormat(new Path(path), factory)
                .withBucketAssigner(new DateTimeBucketAssigner<EventLog>())
                .withBucketCheckInterval(5)
                .withRollingPolicy(OnCheckpointRollingPolicy.build())
                .withOutputFileConfig(
                        OutputFileConfig.builder()
                                .withPartSuffix(".parquet")
                                .withPartPrefix("location").build())
                .build();
//        source.sinkTo(sink3);

        env.execute();
    }
}
