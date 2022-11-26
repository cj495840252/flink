package org.flink.demos.window;

import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.TreeMap;

/**
 * keyed开窗，滑动窗口
 * aggregate，apply，process方法
 *
 *
 */

/*
* 需求：每隔10s，统计最近三十秒num字段的平均值
* 滑动窗口 + aggregate
*
* */
public class Window_demo1 {
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
        watermarks.print();



        SingleOutputStreamOperator<Double> result = watermarks.keyBy(EventBean::getId)
                // 按事件事件的滑动窗口,第一个time为窗口长度，参数2滑动步长，每滑动一次触发一次输出
                .window(SlidingEventTimeWindows.of(Time.seconds(30), Time.seconds(10)))
                .aggregate(new AggregateFunction<EventBean, Tuple2<Integer, Integer>, Double>() {
                    // 第一个是数据类型，第二个是累加器的数据类型，第三个是返回的结果类型
                    @Override
                    public Tuple2<Integer, Integer> createAccumulator() {
                        //初始化累加器
                        return Tuple2.of(0, 0);
                    }

                    @Override
                    public Tuple2<Integer, Integer> add(EventBean eventBean, Tuple2<Integer, Integer> accumulator) {
                        accumulator.setField(accumulator.f0 + 1, 0);//记录数据条数
                        accumulator.setField(accumulator.f1 + eventBean.getNum(), 1);// 汇总num
                        //或者直接new 一个新的
                        // Tuple2<Integer, Long> tuple2 = Tuple2.of(accumulator.f0 + 1, accumulator.f1 + eventBean.getNum());
                        return accumulator;
                    }

                    @Override
                    public Double getResult(Tuple2<Integer, Integer> accumulator) {
                        return accumulator.f1 / (double) accumulator.f0;
                    }

                    @Override
                    public Tuple2<Integer, Integer> merge(Tuple2<Integer, Integer> currAcc, Tuple2<Integer, Integer> acc1) {
                        /*
                         * 在批计算中，shuffle的上游可以做局部聚合，然后吧局部聚和的结果交给下游做全局聚合
                         * 因此，需要提供两个局部聚合的合并逻辑
                         *
                         * 在流式计算中，不存在聚合机制，该方法不会用到.
                         * 若需要流批一体，可以实现
                         * */
                        return Tuple2.of(currAcc.f0 + acc1.f0, currAcc.f1 + acc1.f1);
                    }
                }).setParallelism(1); //这里并行度要设为1，否则发往不同分区了，看不到结果
        result.print();




        //需求2：每隔10s统计最近30s的数据中，行为事件最长的记录
        watermarks.keyBy(EventBean::getId).window(SlidingEventTimeWindows.of(Time.seconds(30),Time.seconds(10)))
                        //泛型1：数据类型。泛型2：输出结果类型，泛型3：key的类型，泛型4：窗口类型
                        .apply(new WindowFunction<EventBean, EventBean, String, TimeWindow>() {
                            @Override
                            public void apply(String s, TimeWindow timeWindow, Iterable<EventBean> iterable, Collector<EventBean> collector) throws Exception {
                                /**
                                  @param s 本次传给窗口的key
                                 * @param timeWindow 窗口的元信息，比如窗口的起始时间和结束时间
                                 * @param iterable 窗口里面的所有数据的迭代器
                                 * @param collector 收集结果
                                 * @return void
                                 */
                                // 建议自己迭代，这样占内存
                                ArrayList<EventBean> beans = new ArrayList<>();
                                for (EventBean bean : iterable) {
                                    beans.add(bean);
                                }
                                Collections.sort(beans, new Comparator<EventBean>() {
                                    @Override
                                    public int compare(EventBean o1, EventBean o2) {
                                        return (int)(o2.getDataTime() - o1.getDataTime());
                                    }
                                });
                                for (int i = 0; i < Math.min(beans.size(), 2); i++) {
                                    collector.collect(beans.get(i));
                                }
                            }
                        });

        // process, 包含apply所有信息，桶apply,窗口触发时,所有数据一起给到
        watermarks.keyBy(EventBean::getDataTime).window(SlidingEventTimeWindows.of(Time.seconds(30),Time.seconds(10)))
                        .process(new ProcessWindowFunction<EventBean, String, Long, TimeWindow>() {
                            @Override
                            public void process(Long aLong, ProcessWindowFunction<EventBean, String, Long, TimeWindow>.Context context,
                                                Iterable<EventBean> iterable, Collector<String> collector) throws Exception {

                                TimeWindow window = context.window();
                                //若本窗口范围,[1000,2000) ,
                                window.maxTimestamp(); // 1999
                                window.getStart();// 1000
                                window.getEnd();//2000
                                // 建议自己迭代，这样占内存
                                ArrayList<EventBean> beans = new ArrayList<>();
                                for (EventBean bean : iterable) {
                                    beans.add(bean);
                                }
                                Collections.sort(beans, new Comparator<EventBean>() {
                                    @Override
                                    public int compare(EventBean o1, EventBean o2) {
                                        return (int)(o2.getDataTime() - o1.getDataTime());
                                    }
                                });

                                // 带上窗口信息
                                for (int i = 0; i < Math.min(beans.size(), 2); i++) {
                                    collector.collect(JSON.toJSONString(beans.get(i))+","+window.getStart()+","+window.getEnd());
                                }
                            }
                        });

        env.execute();
    }
}
