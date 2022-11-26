package org.flink.demos.window;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.evictors.Evictor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.EventTimeTrigger;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.runtime.operators.windowing.TimestampedValue;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

/**
 * 滚动窗口：10一次
 * 若其中Event的msg = click时也触发一次计算
 */
public class window_demo4 {
    public static final Logger LOGGER = LoggerFactory.getLogger(window_demo4.class);
    public static void main(String[] args) throws Exception {

        System.setProperty("log.file","F:\\JavaProject\\flink-test\\log");
        Configuration conf = new Configuration();
        conf.setString("web.log.path","F:\\JavaProject\\flink-test\\log");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);
        DataStreamSource<String> source = env.socketTextStream("up01", 9998);
        LOGGER.warn("加载完成");
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
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .trigger(MyTrigger.create()) //自定义tragger
                .evictor(new MyEvictor(3000,false,"click"))
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



