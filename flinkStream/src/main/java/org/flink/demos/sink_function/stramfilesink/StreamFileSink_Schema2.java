package org.flink.demos.sink_function.stramfilesink;

import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.parquet.ParquetWriterFactory;
import org.apache.flink.formats.parquet.avro.ParquetAvroWriters;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy;
import org.flink.demos.avroschema.EventBean;
import org.flink.demos.source_function.init.EventLog;
import org.flink.demos.source_function.init.MySourceFunction;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class StreamFileSink_Schema2 {
    public static void main(String[] args) throws Exception {
        String path = "file:///F:/JavaProject/flink-test/mode1/src/outfile/";
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage(path+"checkpoint");

        DataStreamSource<EventLog> source = env.addSource(new MySourceFunction());

        ParquetWriterFactory<EventBean> factory = ParquetAvroWriters.forSpecificRecord(EventBean.class);
        FileSink<EventBean> fileSink = FileSink .forBulkFormat(new Path(path + "data"), factory)
                .withRollingPolicy(OnCheckpointRollingPolicy.build())
                .withBucketAssigner(new DateTimeBucketAssigner<EventBean>("yyyy-MM-dd HH点"))
                .withBucketCheckInterval(2000)
                .withOutputFileConfig(OutputFileConfig.builder().withPartPrefix("test")
                        .withPartSuffix(".parquet").build()).build();

        source.map(eventLog -> {
            EventBean bean = new EventBean();
            HashMap<CharSequence, CharSequence> hashMap = new HashMap<>();
            Set<Map.Entry<String, String>> entrySet = eventLog.getEventInfo().entrySet();
            entrySet.forEach(kv->hashMap.put(kv.getKey(), kv.getValue()));
            bean.setEventId(eventLog.getEventId());
            bean.setEventInfo(hashMap);
            bean.setGuid(eventLog.getGuid());
            bean.setSessionId(eventLog.getSessionId());
            bean.setTimeStamp(eventLog.getTimeStamp());
            return bean;
        }).sinkTo(fileSink);

        env.execute();

    }
}
