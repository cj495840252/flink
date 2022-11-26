package com.test.demos;

import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;


/**
 * 测试数据
 * {"guid":1, "eventId":"e02","eventTime":"2022-11-18 15:36:10.300","pageId":"p001"}
 * {"guid":1, "eventId":"e02","eventTime":"2022-11-18 15:36:12.300","pageId":"p002"}
 * {"guid":1, "eventId":"e02","eventTime":"2022-11-18 15:36:14.300","pageId":"p003"}
 * {"guid":1, "eventId":"e02","eventTime":"2022-11-18 15:36:16.300","pageId":"p004"}
 * {"guid":1, "eventId":"e02","eventTime":"2022-11-18 15:36:18.300","pageId":"p005"}
 * {"guid":1, "eventId":"e02","eventTime":"2022-11-18 15:36:20.300","pageId":"p006"}
 * {"guid":1, "eventId":"e02","eventTime":"2022-11-18 15:36:22.300","pageId":"p007"}
 *
 *
 */
public class demo7_EventWatermark {
    public static void main(String[] args) {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment env = StreamTableEnvironment.create(environment);

        env.executeSql("drop table if exists t_event");
        env.executeSql(
                "  create table t_event(                                                  "
                        +"     	    guid int,                                                      "
                        +"     	    eventId string,                                                "
                        +"     	    eventTime timestamp(3),                                        "
                        +"     	    pageId string,                                                 "
                        +"     	    watermark for eventTime as eventTime - interval '1' second               "
                        +"     )WITH(                                                              "
                        +"          'connector' = 'kafka',                                         "
                        +"          'topic' = 'test_event',                                              "
                        +"          'properties.bootstrap.servers' = 'node3:9092',                 "
                        +"          'properties.group.id' = 'testGroup',                           "
                        +"          'scan.startup.mode' = 'earliest-offset',                       "
                        +"          'format' = 'json',                                             "
                        +"          'json.fail-on-missing-field' = 'false',                        "
                        +"          'json.ignore-parse-errors' = 'true'                            "
                        +"           )                                                             "
        );
        env.executeSql("desc t_event").print();
        //env.executeSql("select *,current_watermark(eventTime)as wm from t_event").print();


        /**
         * 测试数据
         * {"guid":1, "eventId":"e02","eventTime":1668756970000,"pageId":"p001"}
         * {"guid":1, "eventId":"e02","eventTime":1668756972000,"pageId":"p002"}
         * {"guid":1, "eventId":"e02","eventTime":1668756974000,"pageId":"p003"}
         * {"guid":1, "eventId":"e02","eventTime":1668756976000,"pageId":"p004"}
         * {"guid":1, "eventId":"e02","eventTime":1668756978000,"pageId":"p005"}
         * {"guid":1, "eventId":"e02","eventTime":1668756979000,"pageId":"p006"}
         * {"guid":1, "eventId":"e02","eventTime":1668756980000,"pageId":"p007"}
         */
        // bigint 类型定义watermark
        env.executeSql("drop table if exists t_event2");
        env.executeSql(
                      "  create table t_event2(                                                 "
                        +"     	    guid int,                                                      "
                        +"     	    eventId string,                                                "
                        +"     	    eventTime bigint,                                              "
                        +"     	    pageId string,                                                 "
                        +"     	    rt as TO_TIMESTAMP_LTZ(eventTime,3),                           "
                        +"     	    watermark for rt as rt - interval '0.001' second               "
                        +"     )WITH(                                                              "
                        +"          'connector' = 'kafka',                                         "
                        +"          'topic' = 'test_event2',                                       "
                        +"          'properties.bootstrap.servers' = 'node3:9092',                 "
                        +"          'properties.group.id' = 'testGroup',                           "
                        +"          'scan.startup.mode' = 'earliest-offset',                       "
                        +"          'format' = 'json',                                             "
                        +"          'json.fail-on-missing-field' = 'false',                        "
                        +"          'json.ignore-parse-errors' = 'true'                            "
                        +"           )                                                             "
        );
        env.executeSql("desc t_event2").print();
        //env.executeSql("select *,current_watermark(rt)as wm from t_event2").print();



        // pt as proctime()声明 处理时间
        env.executeSql("drop table if exists t_event2");
        env.executeSql(
                "  create table t_event2(                                                 "
                        +"     	    guid int,                                                      "
                        +"     	    eventId string,                                                "
                        +"     	    eventTime bigint,                                              "
                        +"     	    pageId string,                                                 "
                        +"     	    rt as TO_TIMESTAMP_LTZ(eventTime,3),                           "
                        +"     	    pt as proctime(),                           "
                        +"     	    watermark for rt as rt - interval '0.001' second               "
                        +"     )WITH(                                                              "
                        +"          'connector' = 'kafka',                                         "
                        +"          'topic' = 'test_event2',                                       "
                        +"          'properties.bootstrap.servers' = 'node3:9092',                 "
                        +"          'properties.group.id' = 'testGroup',                           "
                        +"          'scan.startup.mode' = 'earliest-offset',                       "
                        +"          'format' = 'json',                                             "
                        +"          'json.fail-on-missing-field' = 'false',                        "
                        +"          'json.ignore-parse-errors' = 'true'                            "
                        +"           )                                                             "
        );
        env.executeSql("desc t_event2").print();
        env.executeSql("select *,current_watermark(rt)as wm from t_event2").print();
    }



}
