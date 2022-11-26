package com.test.demos;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

public class demo1_sql {
    public static void main(String[] args) {
        EnvironmentSettings settings = EnvironmentSettings.inStreamingMode();
        TableEnvironment env = TableEnvironment.create(settings);

        //StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        //StreamTableEnvironment env1 = StreamTableEnvironment.create(environment);

        //把kafka种的一个topic映射成一张flinksql表
        // kafka source data：{"id":1,"name":"zhang san","age":28,"gender":"male"}
        env.executeSql("create table kafka_to_json(\n" +
                "                  id int,\n" +
                "                  name string,\n" +
                "                  age int,\n" +
                "                  gender string,\n" +
                "                  primary key (id) not enforced\n" +
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

        Table table = env.from("kafka_to_json");
        table.groupBy($("gender"))
                .select($("gender"),$("age").avg().as("abg_age"))
                .execute()
                .print();

        env.executeSql("create table sink(\n" +
                "                  gender string primary key,\n" +
                "                  avg_age double\n" +
                "                ) WITH(\n" +
                "                    'connector' = 'jdbc',\n" +
                "                    'url' = 'jdbc:mysql://192.168.40.42:3306/test?serverTimezone=Asia/Shanghai&useSSL=false',\n" +
                "                    'table-name' = 'sink',\n" +
                "                    'username' = 'root',\n" +
                "                    'password' = 'root'\n" +
                "                )");


        env.executeSql("insert into sink select gender,avg(age) as avg_age from kafka_to_json group by gender");


    }
}
