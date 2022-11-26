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
 * @Description: 流转表，写入kafka,观察表的  -D
 * @author 陈佳
 * @date 2022-11-19
 * test data: 1,male   2,male  3,female
 */

public class demo12_upsertKafka2 {
    public static void main(String[] args) {
        Configuration configuration = new Configuration();
        configuration.setString("rest.port","8081-8085");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);

        DataStreamSource<String> node1 = env.socketTextStream("node1", 9999);
        DataStreamSource<String> node2 = env.socketTextStream("node2", 9999);
        // 1,male
        SingleOutputStreamOperator<Bean1> stream1 = node1.map(s -> {
            String[] split = s.split(",");
            return new Bean1(Integer.parseInt(split[0]), split[1]);
        }).returns(Bean1.class);
        SingleOutputStreamOperator<Bean2> stream2 = node2.map(s -> {
            String[] split = s.split(",");
            return new Bean2(Integer.parseInt(split[0]), Integer.parseInt(split[1]),split[2]);
        }).returns(Bean2.class);

        // 2.流转表
        tenv.createTemporaryView("t1",stream1);
        tenv.createTemporaryView("t2",stream2);

        // sink表
        tenv.executeSql("drop table if exists upsert_kafka");
        tenv.executeSql(
                      "      create table upsert_kafka(                                               "
                        +"      	id int primary key not enforced,                                     "
                        +"      	gender string ,                                                      "
                        +"      	name string                                                          "
                        +"      ) with (                                                                 "
                        +"                'connector' = 'upsert-kafka',                                  "
                        +"                'topic' = 'upsert_kafka',                                      "
                        +"                'properties.bootstrap.servers' = 'node3:9092',                 "
                        +"                'properties.group.id' = 'testGroup',                           "
                        +"                'value.format' = 'csv',     						             "
                        +"                'key.format' = 'csv'                       				     "
                        +"    		  )																	 "
        );

        tenv.executeSql(
                        "insert into upsert_kafka                 " +
                           "select t1.id,t1.gender,t2.name           " +
                           "from t1 left join t2 on t1.id = t2.id    "
            );
        tenv.executeSql("select * from upsert_kafka").print();

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
        public int age;
        public String name;
    }
}
