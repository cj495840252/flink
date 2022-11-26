package org.flink.demos.window;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.*;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.Iterator;

/**
 * 计数一个事务事件窗口中的数据条数，并且大延迟数据输出到侧流
 */
public class window_demo3 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> source = env.socketTextStream("up01", 9999);
        SingleOutputStreamOperator<EventBean> beanStream = source.map(s -> {
            String[] split = s.split("\\s+");
            return new EventBean(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]), split[3]);
        }).returns(EventBean.class);

        SingleOutputStreamOperator<EventBean> watermarks = beanStream.assignTimestampsAndWatermarks(
                WatermarkStrategy.<EventBean>forBoundedOutOfOrderness(Duration.ofMillis(0))
                        .withTimestampAssigner(new SerializableTimestampAssigner<EventBean>() {
                            @Override
                            public long extractTimestamp(EventBean eventBean, long l) {
                                return eventBean.getDataTime();
                            }
                        })
        );

        OutputTag<EventBean> outputTag = new OutputTag<EventBean>("侧流字符串标识", TypeInformation.of(new TypeHint<EventBean>() {}));
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = watermarks.keyBy(EventBean::getId)
                .window(TumblingEventTimeWindows.of(Time.seconds(30)))
                .allowedLateness(Time.seconds(2))//允许迟到2s
                .sideOutputLateData(outputTag) // 超过允许迟到后，输出到侧流
                .apply(new WindowFunction<EventBean, Tuple2<String, Integer>, String, TimeWindow>() {

                    @Override
                    public void apply(String s, TimeWindow timeWindow, Iterable<EventBean> iterable, Collector<Tuple2<String, Integer>> collector) throws Exception {
                        Tuple2<String, Integer> tuple2 = Tuple2.of(s+" "+timeWindow.getStart()+" "+timeWindow.getEnd(), 0);
                        for (EventBean bean : iterable) {
                            tuple2.f1 = tuple2.f1 + 1;
                        }
                        collector.collect(tuple2);
                    }
                }).returns(new TypeHint<Tuple2<String, Integer>>() {});

        result.getSideOutput(outputTag).print("侧流 -> ");
        result.print("keyed窗口数据条数 ->");

        env.execute();
    }
}

