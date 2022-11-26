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

public class demo15_timewindow {
    public static void main(String[] args) {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tenv = StreamTableEnvironment.create(environment);
        environment.setParallelism(1);
        DataStreamSource<String> source = environment.socketTextStream("node1", 9999);

        // 1.获取流
        SingleOutputStreamOperator<Bid> map = source.map(s -> {
            String[] split = s.split(",");
            return new Bid(split[0], Double.parseDouble(split[1]), split[2]);
        }).returns(Bid.class);

        // 2.流转表
        tenv.createTemporaryView("bid",map, Schema.newBuilder()
                        .column("bidtime", DataTypes.STRING())
                        .column("price", DataTypes.DOUBLE())
                        .column("item", DataTypes.STRING())
                        .columnByExpression("rt",$("bidtime").toTimestamp())
                        .watermark("rt","rt - interval '1' second")
                .build());

        // 3. 查询
        /** tenv.executeSql("select rt,price,item,current_watermark(rt) as wm from bid ").print();*/
        // 滚动窗口：每隔一分钟，计算过去五分钟的总额，
        /** tenv.executeSql(
                     "   select                                                                                 "+
                        "   	window_start,                                                                      "+
                        "   	window_end,                                                                        "+
                        "   	sum(price) as amt                                                                  "+
                        "    from table(                                                                           "+
                        "   	hop(table bid, descriptor(rt), interval '1' minutes, interval '50' minutes)        "+
                        "   )group by window_start,window_end                                                      "
        ).print();*/
        // 每两分钟计算今天以来的总交易额
        /** tenv.executeSql(
                     "   select                                                                                 "+
                        "   	window_start,                                                                      "+
                        "   	window_end,                                                                        "+
                        "   	sum(price) as amt                                                                  "+
                        "    from table(                                                                           "+
                        "   	cumulate(table bid, descriptor(rt), interval '2' minutes, interval '24' hour)      "+
                        "   )group by window_start,window_end                                                      "
        ).print();*/


    }




@Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Bid{
        private  String bidtime;
        private  double price ;
        private  String item ;

    }
}
