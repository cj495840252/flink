package com.test.demos;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

/**
 * @Description:  测试：当带watermark的表转成流时，watermark策略
 * @result   结果会自动转成watermark，并且不会再减去1ms
 * @author 陈佳
 * @date 2022-11-19
 *
 * data:
 * {"guid":1, "eventId":"e02","eventTime":1668756970000,"pageId":"p001"}
 * {"guid":1, "eventId":"e02","eventTime":1668756972000,"pageId":"p002"}
 * {"guid":1, "eventId":"e02","eventTime":1668756974000,"pageId":"p003"}
 * {"guid":1, "eventId":"e02","eventTime":1668756976000,"pageId":"p004"}
 * {"guid":1, "eventId":"e02","eventTime":1668756978000,"pageId":"p005"}
 * {"guid":1, "eventId":"e02","eventTime":1668756979000,"pageId":"p006"}
 * {"guid":1, "eventId":"e02","eventTime":1668756980000,"pageId":"p007"}
 */


public class demo9_tableToStreamWatermark {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        StreamTableEnvironment env = StreamTableEnvironment.create(environment);

        env.executeSql("drop table if exists t_event");
        env.executeSql("create table if not exists t_event(          " +
                "                  guid int,                                            " +
                "                  eventId string,                                      " +
                "                  eventTime bigint,                                    " +
                "                  pageId string,                                       " +
                "                  pt as proctime(),                                    " +
                "                  rt as to_timestamp_ltz(eventTime,3),                 " +
                "                  watermark for rt as rt - interval '1' second         " +
                "                ) WITH(                                                " +
                "                    'connector' = 'kafka',                             " +
                "                    'topic' = 'test_event2',                           " +
                "                    'properties.bootstrap.servers' = 'node3:9092',     " +
                "                    'properties.group.id' = 'testGroup',               " +
                "                    'scan.startup.mode' = 'earliest-offset',           " +
                "                    'format' = 'json',                                 " +
                "                    'json.fail-on-missing-field' = 'false',            " +
                "                    'json.ignore-parse-errors' = 'true'                " +
                "                )");
        //env.executeSql("select *,current_watermark(rt) from t_event").print();


        DataStream<Row> eventStream = env.toDataStream(env.from("t_event"));
        DataStreamSink<String> print = eventStream.process(new ProcessFunction<Row, String>() {
            @Override
            public void processElement(Row row, ProcessFunction<Row, String>.Context context, Collector<String> collector) throws Exception {
                String s = row.toString() + "  watermark:" + context.timerService().currentWatermark()
                        + "  processTime：" + context.timerService().currentProcessingTime();
                collector.collect(s);
            }
        }).print();
        environment.execute();
    }
}
