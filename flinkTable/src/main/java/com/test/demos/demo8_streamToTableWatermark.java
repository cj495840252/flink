package com.test.demos;

import com.alibaba.fastjson.JSON;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @Description:  当一个流转表时，在流中定义的watermark继承到表中的方法
 * @author :陈佳
 * @date 2022-11-19
 */
public class demo8_streamToTableWatermark {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> source = env.socketTextStream("node1", 9999);
        env.setParallelism(1);

        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);

        SingleOutputStreamOperator<EventBean> map = source.map(s -> JSON.parseObject(s, EventBean.class)).returns(EventBean.class);
        SingleOutputStreamOperator<EventBean> watermarks = map.assignTimestampsAndWatermarks(WatermarkStrategy.<EventBean>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<EventBean>() {
                    @Override
                    public long extractTimestamp(EventBean eventBean, long l) {
                        return eventBean.eventTime;
                    }
                }));

        /*观察watermark的推进*/
        /** SingleOutputStreamOperator<String> process = watermarks.process(new ProcessFunction<EventBean, String>() {
            @Override
            public void processElement(EventBean eventBean, ProcessFunction<EventBean, String>.Context context, Collector<String> collector) throws Exception {
                String s = eventBean.toString()+"    当前水位线 " + context.timerService().currentWatermark();
                collector.collect(s);
            }
        process.print();
        });*/

        // 1.流转表后，表取不到流中的watermark
        tenv.executeSql("drop table if exists streamTable1");
        tenv.executeSql("drop table if exists streamTable2");
        tenv.executeSql("drop table if exists streamTable3");
        tenv.executeSql("drop table if exists streamTable4");
        tenv.createTemporaryView("streamTable1",watermarks);
        // 2.带schema,相当于重新定义
        tenv.createTemporaryView("streamTable2",watermarks,
                Schema.newBuilder()
                        .column("guid", DataTypes.INT())
                        .column("eventId", DataTypes.STRING())
                        .column("eventTime", DataTypes.BIGINT())
                        .column("pageId", DataTypes.STRING())
                        .columnByExpression("rt","TO_TIMESTAMP_LTZ(eventTime,3)")
                        .watermark("rt","rt - interval '1' second")
                        .build());
        //  3.1 沿用流中watermark方法1，元数据获取,也相当于重新创建
        tenv.createTemporaryView("streamTable3",watermarks,
                Schema.newBuilder()
                        .column("guid", DataTypes.INT())
                        .column("eventId", DataTypes.STRING())
                        .column("eventTime", DataTypes.BIGINT())
                        .column("pageId", DataTypes.STRING())
                        .columnByMetadata("rt",DataTypes.TIMESTAMP_LTZ(3),"rowtime")
                        .watermark("rt","rt - interval '1' second")
                        .build());
        //  3.1 沿用流中watermark，底层方法+元数据获取，继承流上的watermark
        tenv.createTemporaryView("streamTable4",watermarks,
                Schema.newBuilder()
                        .column("guid", DataTypes.INT())
                        .column("eventId", DataTypes.STRING())
                        .column("eventTime", DataTypes.BIGINT())
                        .column("pageId", DataTypes.STRING())
                        .columnByMetadata("rt",DataTypes.TIMESTAMP_LTZ(3),"rowtime")
                        .watermark("rt","source_watermark()")
                        .build());


        tenv.executeSql("select guid,eventId,eventTime,pageId,current_watermark(rt)as wm from streamTable4").print();


        /* // 表转流，测试流转表后转回流watermark存不存在： 结论：丢失
        DataStream<Row> tableStream = tenv.toDataStream(tenv.from("streamTable"));
        SingleOutputStreamOperator<String> process = tableStream.process(new ProcessFunction<Row, String>() {
            @Override
            public void processElement(Row row, ProcessFunction<Row, String>.Context context, Collector<String> collector) throws Exception {
                row.getKind();//得到流的类型，+I -D +U —U
                collector.collect(row + "   watermark: " + context.timerService().currentWatermark());
            }
        });
        process.print();*/

        env.execute();
    }




    @NoArgsConstructor
    @AllArgsConstructor
    public static class EventBean {
        public int guid;
        public String eventId;
        public long eventTime;
        public String pageId;

        @Override
        public String toString() {
            return "{" +
                    "guid: " + guid +
                    ", eventId: '" + eventId + '\'' +
                    ", eventTime: " + eventTime +
                    ", pageId: '" + pageId + '\'' +
                    '}';
        }
    }
}
