package org.flink.demos.sink_function.stramfilesink;

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
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy;

import org.flink.demos.avroschema.EventBean;
import org.flink.demos.source_function.init.EventLog;
import org.flink.demos.source_function.init.MySourceFunction;


public class StreamFileSink_Schema1 {
    public static void  main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        String path = "file:///F:/JavaProject/flink-test/mode1/src/outfile/";
        // 开启checkpoint
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage(path+"checkpoint");

        // 构造一个数据流
        DataStreamSource<EventLog> source =  env.addSource(new MySourceFunction());



        //将上面的数据流输出到文件系统

        // 1.获取schema=>定义GenericRecord
        Schema schema = SchemaBuilder.builder()
                .record("DataRecord")
                .namespace("org.flink.demos.avro.schema")
                .doc("用户行为事件模式")// 文档注释
                .fields()
                .requiredInt("gid")//名称可以不一样
                .requiredString("sessionID")
                .requiredString("eventID")
                .requiredLong("timestamp")
                //map类型
                .name("eventInfo")
                    .type()
                    .map()
                    .values()
                    .type("string")
                    .noDefault()
                .endRecord();


        // 2.这里得到写出的sink，但是sink写出时要求每条数据为GenericRecord类型

        Schema schema1 = EventBean.getClassSchema();
        ParquetWriterFactory<GenericRecord> factory =ParquetAvroWriters.forGenericRecord(schema);

        FileSink<GenericRecord> fileSink = FileSink.forBulkFormat(new Path(path +"data"), factory)
                .withRollingPolicy(OnCheckpointRollingPolicy.build())
                .withBucketAssigner(new DateTimeBucketAssigner<GenericRecord>("yyyy-MM-dd HH点"))
                .withBucketCheckInterval(2000)
                .withOutputFileConfig(OutputFileConfig.builder()
                        .withPartPrefix("test_file")
                        .withPartSuffix(".parquet").build())
                .build();





        // 3.将每条数据转化成GenericRecord
        DataStream<GenericRecord> source1 = source.map((MapFunction<EventLog, GenericRecord>) eventLog -> {
            GenericData.Record record = new GenericData.Record(schema);

            record.put("gid",eventLog.getGuid());
            record.put("sessionID",eventLog.getSessionId());
            record.put("eventID",eventLog.getEventId());
            record.put("timestamp",eventLog.getTimeStamp());
            record.put("eventInfo",eventLog.getEventInfo());
            return record;
        }).returns(new GenericRecordAvroTypeInfo(schema));//returns的时候需要lambda表达式，否则return不会生效

        // 4.写出
        source1.sinkTo(fileSink);//缺一个sink，去获取
        env.execute();
    }
}
