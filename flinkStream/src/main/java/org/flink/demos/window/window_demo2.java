package org.flink.demos.window;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.*;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * 各种开窗API
 * 全局 计数滑动窗口
 * 全局 计数滚动窗口
 * 全局 事件时间滑动窗口
 * 全局 事件时间滚动窗口
 * 全局 事件时间会话窗口
 * 全局 处理时间滑动窗口
 * 全局 处理时间滚动窗口
 * 全局 处理时间会话窗口
 *
 *  keyed 计数滑动窗口
 *  keyed 计数滚动窗口
 *  keyed 事件时间滑动窗口
 *  keyed 事件时间滚动窗口
 *  keyed 事件时间会话窗口
 *  keyed 处理时间滑动窗口
 *  keyed 处理时间滚动窗口
 *  keyed 处理时间会话窗口

 */

public class window_demo2 {
    public static void main(String[] args) {
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
        watermarks.print();


        // 全局 计数滑动窗口
        beanStream.countWindowAll(10)// 窗口长度10条数据
                .apply(new AllWindowFunction<EventBean, String, GlobalWindow>() {
            @Override
            public void apply(GlobalWindow globalWindow, Iterable<EventBean> iterable, Collector<String> collector) throws Exception {

            }
        });
        // 全局 计数滚动窗口
        beanStream.countWindowAll(10,2);//窗口长度10条数据，滑动步长为2条数据
                /*.apply()*/

        // 全局 事件时间滑动窗口
        watermarks.windowAll(TumblingEventTimeWindows.of(Time.seconds(30)))
                .apply(new AllWindowFunction<EventBean, String, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow timeWindow, Iterable<EventBean> iterable, Collector<String> collector) throws Exception {

                    }
                });
        // 全局 事件时间滚动窗口
        watermarks.windowAll(SlidingEventTimeWindows.of(Time.seconds(30),Time.seconds(10)));
                /*.apply()*/
        // 全局 事件时间会话窗口
        watermarks.windowAll(EventTimeSessionWindows.withGap(Time.seconds(30))); //前后两条数据超过30s划分窗口
        // 全局 处理时间滑动窗口
        beanStream.windowAll(SlidingProcessingTimeWindows.of(Time.seconds(30),Time.seconds(10)));
        // 全局 处理时间滚动窗口
        beanStream.windowAll(TumblingProcessingTimeWindows.of(Time.seconds(30)));
        // 全局 处理时间会话窗口
        beanStream.windowAll(ProcessingTimeSessionWindows.withGap(Time.seconds(30)));


        KeyedStream<EventBean, String> keyBy = watermarks.keyBy(EventBean::getId);
        //  keyed 计数滑动窗口
        keyBy.countWindow(10);
        //  keyed 计数滚动窗口
        keyBy.countWindow(10,2);
        //  keyed 事件时间滑动窗口
        keyBy.window(SlidingEventTimeWindows.of(Time.seconds(30),Time.seconds(10)));
        //  keyed 事件时间滚动窗口
        keyBy.window(TumblingEventTimeWindows.of(Time.seconds(30)));
        //  keyed 事件时间会话窗口
        keyBy.window(EventTimeSessionWindows.withGap(Time.seconds(30)));
        //  keyed 处理时间滑动窗口
        keyBy.window(SlidingProcessingTimeWindows.of(Time.seconds(30),Time.seconds(10)));
        //  keyed 处理时间滚动窗口
        keyBy.window(TumblingProcessingTimeWindows.of(Time.seconds(30)));
        //  keyed 处理时间会话窗口
        keyBy.window(ProcessingTimeSessionWindows.withGap(Time.seconds(30)));

    }
}
