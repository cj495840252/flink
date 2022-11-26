package com.test.demos;

import com.alibaba.fastjson.JSON;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;


public class demo3_mix {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> source = environment.fromElements("{\"id\":2,\"dataTime\":10212154,\"num\":26,\"msg\":\"male\"}");
        environment.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        StreamTableEnvironment env = StreamTableEnvironment.create(environment);
        SingleOutputStreamOperator<EventBean> map = source.map(s -> JSON.parseObject(s, EventBean.class));



        // 1.直接创建表，不带表名
        /*Table table = env.fromDataStream(map,
                Schema.newBuilder()
                        .column("id",DataTypes.STRING())
                        .column("dataTime",DataTypes.BIGINT())
                        .column("num",DataTypes.INT())
                        .column("msg",DataTypes.STRING())
                        .build());*/
        // 和上面的注释是一样的
        Table table = env.fromDataStream(map);
        table.execute().print();


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

        env.listCatalogs();

        /*table1.printSchema();
        table1.execute().print();*/

        // 2. 带表名的创建
        /*env.createTable("table_name",
                TableDescriptor.forConnector("kafka")
                        .format("csv")
                        .option("","")
                        .schema(Schema.newBuilder().build())
                        .build());*/

        // 2.1 从dataStream创建带表名,也可以填入schema
        //env.createTemporaryView("view_name",map);

    }
    @NoArgsConstructor
    @AllArgsConstructor
    public static class EventBean {
        public String id;
        public long dataTime;
        public int num;
        public String msg;
    }
}
