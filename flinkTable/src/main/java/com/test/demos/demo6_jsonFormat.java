package com.test.demos;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * json format 高级
 */
public class demo6_jsonFormat {
    public static void main(String[] args) {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.inBatchMode();
        StreamTableEnvironment env = StreamTableEnvironment.create(environment,settings);


        env.executeSql("drop table if exists test_json");
        env.executeSql("	create table test_json(                                     "
                + "		id int,                                             "
                + "		name map<String,String>,                            "
                + "		bigid as id*10                                      "
                + "	    )with(                                              "
                + "		'connector' = 'filesystem',                         "
                + "		'path' = 'F:\\JavaProject\\flink-test\\TestData\\json.qiantao\\test1',   " //这里正常情况下应该指定目录而不是文件
                + "		'format' = 'json'                                   "
                + "	)                                                       ");
        env.executeSql("select id,name['nick']as nick from test_json").print();

        // tableApi, Row类型更灵活，map中的k v都需要是字符串
        env.executeSql("drop table if exists test_json2");
        env.createTable("test_j", TableDescriptor.forConnector("filesystem")
                        .schema(Schema.newBuilder()
                                .column("id", DataTypes.INT())
                                .column("name",DataTypes.ROW(
                                        DataTypes.FIELD("nick",DataTypes.STRING()),
                                        DataTypes.FIELD("formal",DataTypes.STRING()),
                                        DataTypes.FIELD("height",DataTypes.INT())
                                ))
                                .columnByExpression("bigid","id*10")
                                .build())
                        .format("json")
                        .option("path","F:\\JavaProject\\flink-test\\TestData\\json.qiantao\\test2")
                .build());
        env.executeSql("select *  from test_j").print();
        env.executeSql("select id,name['nick'],name['height'] as nick from test_j").print();

        // 更复杂json
        env.executeSql("drop table if exists test_json3");
        env.executeSql(
                      "	create table test_json3(                                              "
                        +"		id int,                                                           "
                        +"		friends array<row<name string,info map<string,string>>>           "
                        +"	)with(                                                                "
                        +"		'connector' = 'filesystem',                                       "
                        +"		'path' = 'F:/JavaProject/flink-test/TestData/json.qiantao/test3', "
                        +"		'format' = 'json'                                                 "
                        +"	)                                                                     "
        );
        env.executeSql("select * from test_json3").print();
        env.executeSql(
                      " select id,                                "
                        +"	friends[1].name as name,                 "
                        +"	friends[1].info['address'] as address,      "
                        +"	friends[1].info['gender'] as gender      "
                        +" from test_json3                           "
        ).print();
        env.executeSql(
                " select id,                                "
                        +"	friends[1].name as name,                 "
                        +"	friends[1]['info']['address'] as address,      "
                        +"	friends[1]['info']['gender'] as gender      "
                        +" from test_json3                           "
        ).print();
    }
}
