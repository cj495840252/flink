package com.test.demos;


import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * 读取kafka的所有数据，带key，value，headers
 * @author 陈佳
 * @date 2022-11-19
 *
 * test data
 * key {"k1": 2,"k2": "v2"}
 * value {"guid":1, "eventId":"e02","eventTime":1668756980000,"pageId":"p001","k1": 2,"k2": "v3"}
 * headers heads1: headersvalue1,heads2: headersvalue1
 */
public class demo10_connecter {
    public static void main(String[] args) {

        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        StreamTableEnvironment tenv = StreamTableEnvironment.create(environment);

        tenv.executeSql("drop table if exists kafkaTable");
        tenv.executeSql(
                     "   create table kafkaTable(                                          "+
                        "   	`meta_time` timestamp(3) metadata from 'timestamp',           "+
                        "   	`partition` bigint metadata virtual,                          "+
                        "   	`offset` 	bigint metadata virtual,                          "+
                        "   	`inKey_k1`  bigint,                                           "+
                        "   	`inKey_k2`  string,                                           "+
                        "   	`guid` 		bigint,                                           "+
                        "   	`eventId`   string,                                           "+
                        "   	`k1`		bigint,                                           "+
                        "   	`k2`		string,                                           "+
                        "   	`eventTime` bigint,                                           "+
                        "   	`headers`	map<string,BYTES> metadata FROM 'headers'         "+
                        "   )with(                                                            "+
                        "   	'connector' = 'kafka',                                        "+
                        "   	'topic' = 'test_kafka',                                       "+
                        "   	'properties.bootstrap.servers' = 'node3:9092',                "+
                        "   	'properties.group.id' = 'testGroup',                          "+
                        "   	'scan.startup.mode' = 'earliest-offset',                      "+
                        "   	'key.format' = 'json',                                        "+
                        "   	'key.json.fail-on-missing-field' = 'false',                   "+
                        "   	'key.json.ignore-parse-errors' = 'true' ,                     "+
                        "   	'key.fields'='inKey_k1;inKey_k2',                             "+
                        "   	'key.fields-prefix'='inKey_',                                 "+
                        "   	'value.format' = 'json',                                      "+
                        "   	'value.json.fail-on-missing-field' = 'false',                 "+
                        "   	'value.fields-include' = 'EXCEPT_KEY'                         "+
                        "                                                                     "+
                        "    )                                                                "
        );

        //tenv.executeSql("select * from kafkaTable").print();
        tenv.executeSql("select guid,eventId,eventTime,cast(headers['head2'] as string) as headers  from kafkaTable").print();
    }
}
