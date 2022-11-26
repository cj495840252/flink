package com.test.demos;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import static org.apache.flink.table.api.Expressions.$;

/**
 *
 *
 *        test data:
 *        2020-04-15 08:05:00.000,4.00,A,supplier1
 *        2020-04-15 08:06:00.000,4.00,C,supplier2
 *        2020-04-15 08:07:00.000,2.00,G,supplier1
 *        2020-04-15 08:08:00.000,2.00,B,supplier3
 *        2020-04-15 08:09:00.000,5.00,D,supplier4
 *        2020-04-15 08:11:00.000,2.00,B,supplier3
 *        2020-04-15 08:13:00.000,1.00,E,supplier1
 *        2020-04-15 08:15:00.000,3.00,H,supplier2
 *        2020-04-15 08:17:00.000,6.00,F,supplier5
 */
public class demo16_topN {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tenv = StreamTableEnvironment.create(environment);
        environment.setParallelism(1);
        DataStreamSource<String> source = environment.socketTextStream("node1", 9999);

        // 1.获取流
        SingleOutputStreamOperator<Bid1> map = source.map(s -> {
            String[] split = s.split(",");
            return new Bid1(split[0], Double.parseDouble(split[1]), split[2],split[3]);
        }).returns(Bid1.class);




        // 2.流转表
        tenv.executeSql("drop table if exists bid");
        tenv.createTemporaryView("bid",map, Schema.newBuilder()
                        .column("bidtime", DataTypes.STRING())
                        .column("price", DataTypes.DOUBLE())
                        .column("item", DataTypes.STRING())
                        .column("supplier_id", DataTypes.STRING())
                        .columnByExpression("rt",$("bidtime").toTimestamp())
                        .watermark("rt","rt - interval '1' second")
                .build());


        // 分组topN
        tenv.executeSql("" +
                "SELECT *                                                                                               \n" +
                "  FROM (                                                                                               \n" +
                "    SELECT *, ROW_NUMBER() OVER (PARTITION BY window_start, window_end ORDER BY price DESC) as rownum  \n" +
                "    FROM (                                                                                             \n" +
                "      SELECT window_start, window_end, supplier_id, SUM(price) as price, COUNT(*) as cnt               \n" +
                "      FROM TABLE(                                                                                      \n" +
                "        TUMBLE(TABLE bid, DESCRIPTOR(rt), INTERVAL '10' MINUTES))                                      \n" +
                "      GROUP BY window_start, window_end, supplier_id                                                   \n" +
                "    )                                                                                                  \n" +
                "  ) WHERE rownum <= 3").print();

    }




@Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Bid1{
        private  String bidtime;
        private  double price ;
        private  String item ;
        private  String supplier_id;

    }
}
