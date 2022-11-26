package com.test.demos;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.ApiExpression;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.e;

public class demo5_tablefiled {
    public static void main(String[] args) {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment env = StreamTableEnvironment.create(environment);

        // 1.SQL语法的字段定义
        env.executeSql("drop table if exists kafka_to_json");
        env.executeSql("create table if not exists kafka_to_json(\n" +
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


        // 2.table语法的字段定义
        env.executeSql("drop table if exists kafka_to_json2");
        env.createTable("kafka_to_json2",
                TableDescriptor.forConnector("kafka")
                        .format("json")
                        .schema(Schema
                                .newBuilder()
                                .column("id", DataTypes.INT())
                                .column("name", DataTypes.STRING())
                                .column("age", DataTypes.INT())
                                .column("gender", DataTypes.STRING())
                                .columnByExpression("id1","id+10")
                                //.columnByExpression("id2",$("id").plus(10)) 目前语法上正确，但是不支持
                                //isVirtual是表示，当这个表作为sink表时,不显示该字段。就是插入数据到这张表时，不插入这个字段
                                .columnByMetadata("offset",DataTypes.BIGINT(),"offset",false)
                                // TIMESTAMP_LTZ(3),显示的时候格式为2022-11-17 13:45:39.503，3表示精确到毫秒
                                .columnByMetadata("ts",DataTypes.TIMESTAMP_LTZ(3),"timestamp",true)
                                //.primaryKey("id","name")
                                .build())
                        .option("topic","json")
                        .option("properties.bootstrap.servers","node3:9092")
                        .option("properties.group.id","testGroup")
                        .option("scan.startup.mode","earliest-offset")
                        .option("json.fail-on-missing-field","false")
                        .option("json.ignore-parse-errors","true")
                        .build()
        );
        env.executeSql("desc kafka_to_json2").print();
        env.executeSql("select * from kafka_to_json2").print();

    }
}
