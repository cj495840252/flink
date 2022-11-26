package com.test.demos;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author 陈佳
 * @date 2022-11-20
 * @Description: flink sql table 的scan模式，
 *                 1. 做source表。有界流
 *                 2. 做sink表，支持更新，  测试方法两流join写入mysql
 */
public class demo14_jdbcSink {
    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        configuration.setString("rest.port","8081-8085");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);

        tenv.executeSql("drop table if exists test_jdbc");
        tenv.executeSql(
                     "       create table test_jdbc(                                                             " +
                        "       	id int primary key,                                                             " +
                        "       	name string,                                                                    " +
                        "       	age int,                                                                        " +
                        "       	gender string                                                                   " +
                        "       ) with(                                                                             " +
                        "       	'connector' = 'jdbc',                                                           " +
                        "       	'url' = 'jdbc:mysql://localhost:3306/test?serverTimezone=GMT%2B8&useSSL=true',  " +
                        "           'table-name' = 'test_jdbc',                                                     " +
                        "       	'username' = 'root',                                                            " +
                        "       	'password' = 'root'                                                             " +
                        "       )                                                                                   "
        );

        // 1.作为source，只能作为无界流，读完了就没了
        //tenv.executeSql("select * from test_jdbc").print();
        // 2.作为sink流
        DataStreamSource<String> node1 = env.socketTextStream("node1", 9999);
        DataStreamSource<String> node2 = env.socketTextStream("node2", 9999);
        // 1,male
        SingleOutputStreamOperator<demo12_upsertKafka2.Bean1> stream1 = node1.map(s -> {
            String[] split = s.split(",");
            return new demo12_upsertKafka2.Bean1(Integer.parseInt(split[0]), split[1]);
        }).returns(demo12_upsertKafka2.Bean1.class);
        SingleOutputStreamOperator<demo12_upsertKafka2.Bean2> stream2 = node2.map(s -> {
            String[] split = s.split(",");
            return new demo12_upsertKafka2.Bean2(Integer.parseInt(split[0]), Integer.parseInt(split[1]),split[2]);
        }).returns(demo12_upsertKafka2.Bean2.class);


        // 2.流转表
        tenv.createTemporaryView("t1",stream1);
        tenv.createTemporaryView("t2",stream2);

        tenv.executeSql("insert into test_jdbc " +
                "select t1.id,t2.name,t2.age,t1.gender " +
                "from t1 left join t2 on t1.id = t2.id");

        //tenv.executeSql("select *from test_jdbc").print();
    }
}
