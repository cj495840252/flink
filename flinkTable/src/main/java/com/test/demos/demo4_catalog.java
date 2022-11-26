package com.test.demos;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.util.Arrays;
import java.util.Random;

/**
 * 连接hive源数据
 **/
public class demo4_catalog {
    public static void main(String[] args) throws IOException {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment env = StreamTableEnvironment.create(environment);

        HiveCatalog hiveCatalog = new HiveCatalog("hive", "default", "./conf/hiveconf/");
        env.registerCatalog("hive",hiveCatalog);
        env.useCatalog("hive");
        env.executeSql("show databases").print();
        env.executeSql("show tables").print();
        env.executeSql("drop table if exists hive.test.event");
        //env.executeSql("select * from test.user_info").print();


        Table table1 = env.fromValues(
                DataTypes.ROW(
                        DataTypes.FIELD("id", DataTypes.STRING()),
                        DataTypes.FIELD("dataTime", DataTypes.INT()),
                        DataTypes.FIELD("num", DataTypes.INT()),
                        DataTypes.FIELD("msg", DataTypes.STRING())
                ),
                Row.of("001",10000,25,"message1"),
                Row.of("002",14700,23,"message2"),
                Row.of("003",10200,31,"message3")
        );
        env.createTemporaryView("event",table1);

        //创建一个hive中的视图,hive只能看见表名，不能查询
        env.executeSql("create view if not exists hive.test.event_view as select * from event");

        // 创建一个hive中的表,hive只能看见表名，不能查询
        env.executeSql("drop table if exists hive.test.kafka_to_json");
        env.executeSql("create table if not exists hive.test.kafka_to_json(\n" +
                "                  id int,\n" +
                "                  name string,\n" +
                "                  age int,\n" +
                "                  gender string,\n" +
                "                  `offset` BIGINT  METADATA VIRTUAL,\n" +
                "                  ts TIMESTAMP_LTZ(3) METADATA FROM 'timestamp'\n" +
                "                ) WITH(\n" +
                "                    'connector' = 'kafka',\n" +
                "                    'topic' = 'json',\n" +
                "                    'properties.bootstrap.servers' = 'node3:9092',\n" +
                "                    'properties.group.id' = 'testGroup',\n" +
                "                    'scan.startup.mode' = 'earliest-offset',\n" +
                "                    'format' = 'json',\n" +
                "                    'json.fail-on-missing-field' = 'false',\n" +
                "                    'json.ignore-parse-errors' = 'true'\n" +
                "                )");

        // hive中不能查看，这里看看看不看得见
        //env.from("hive.test.event_view").execute().print();
        env.from("hive.test.kafka_to_json").execute().print();




        String[] catalogs = env.listCatalogs();
        System.out.println(Arrays.toString(catalogs));

    }
}
