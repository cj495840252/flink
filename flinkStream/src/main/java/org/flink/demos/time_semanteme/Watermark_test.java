package org.flink.demos.time_semanteme;

import com.alibaba.fastjson.JSON;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @Description:  这个demo观察watermark的输出
 * @author :陈佳
 * @date 2022-11-19
 *
 * 测试数据
 * {"guid":1, "eventId":"e02","eventTime":1668756970000,"pageId":"p001"}
 * {"guid":1, "eventId":"e02","eventTime":1668756972000,"pageId":"p002"}
 * {"guid":1, "eventId":"e02","eventTime":1668756974000,"pageId":"p003"}
 * {"guid":1, "eventId":"e02","eventTime":1668756976000,"pageId":"p004"}
 * {"guid":1, "eventId":"e02","eventTime":1668756978000,"pageId":"p005"}
 * {"guid":1, "eventId":"e02","eventTime":1668756979000,"pageId":"p006"}
 * {"guid":1, "eventId":"e02","eventTime":1668756980000,"pageId":"p007"}
 */
public class Watermark_test {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> source = env.socketTextStream("node1", 9999);
        env.setParallelism(1);
        SingleOutputStreamOperator<EventBean> map = source.map(s -> JSON.parseObject(s, EventBean.class)).returns(EventBean.class);
        SingleOutputStreamOperator<EventBean> watermarks = map.assignTimestampsAndWatermarks(WatermarkStrategy.<EventBean>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<EventBean>() {
                    @Override
                    public long extractTimestamp(EventBean eventBean, long l) {
                        return eventBean.eventTime;
                    }
                }));


        SingleOutputStreamOperator<String> process = watermarks.process(new ProcessFunction<EventBean, String>() {
            @Override
            public void processElement(EventBean eventBean, ProcessFunction<EventBean, String>.Context context, Collector<String> collector) throws Exception {
                String s = eventBean.toString()+"    当前水位线 " + context.timerService().currentWatermark();
                collector.collect(s);
            }
        });
        process.print();
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
