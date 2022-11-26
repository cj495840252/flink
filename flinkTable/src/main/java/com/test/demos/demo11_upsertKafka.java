package com.test.demos;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Description: 单流，流转表 group by，写入kafka,测试changelog
 * @author 陈佳
 * @date 2022-11-19
 * test data: 1,male   2,male  3,female
 */

public class demo11_upsertKafka {
    public static void main(String[] args) {
        Configuration configuration = new Configuration();
        configuration.setString("rest.port","8081-8085");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);

        DataStreamSource<String> node1 = env.socketTextStream("node1", 9999);
        // 1,male
        SingleOutputStreamOperator<Bean1> stream1 = node1.map(s -> {
            String[] split = s.split(",");
            return new Bean1(Integer.parseInt(split[0]), split[1]);
        }).returns(Bean1.class);

        // 2.流转表
        tenv.createTemporaryView("t1",stream1);
        // tenv.executeSql("select gender,count(1) from t1 group by gender").print();

        //
        tenv.executeSql(
                      "      create table upsert_kafka(                                               "
                        +"      	gender string primary key not enforced,                              "
                        +"      	cnt bigint                                                           "
                        +"      ) with (                                                                 "
                        +"                'connector' = 'upsert-kafka',                                  "
                        +"                'topic' = 'upsert_kafka',                                      "
                        +"                'properties.bootstrap.servers' = 'node3:9092',                 "
                        +"                'properties.group.id' = 'testGroup',                           "
                        +"                'value.format' = 'csv',     						             "
                        +"                'key.format' = 'csv'                       				     "
                        +"    		  )																	 "
        );

        tenv.executeSql("insert into upsert_kafka select gender,count(1) from t1 group by gender");

        tenv.executeSql("select *from upsert_kafka").print();
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Bean1{
        public int id;
        public String gender;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Bean2{
        public int id;
        public String gender;
    }
}
