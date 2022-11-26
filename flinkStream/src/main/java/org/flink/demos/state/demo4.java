package org.flink.demos.state;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;

/**
 *
 */
public class demo4 {
    public static void main(String[] args) throws Exception {
        System.setProperty("log.file","./webui");
        Configuration conf = new Configuration();
        conf.setString("web.log.path","./webui");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);
        // 开启checkpoint，默认精确一次
        env.enableCheckpointing(1000, CheckpointingMode.EXACTLY_ONCE);
        //env.getCheckpointConfig().setCheckpointStorage(new URI("hdfs://test"));
        env.getCheckpointConfig().setCheckpointStorage("file:///F:/JavaProject/flink-test/mode1/src/outfile/checkpoint");


        // task级别的failover 默认不重启
        // env.setRestartStrategy(RestartStrategies.noRestart());
        // 固定重启上限，两次重启之间的间隔
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,1000));

        DataStreamSource<String> source = env.socketTextStream("up01", 9998);
        source.keyBy(s->s)
                .map(new RichMapFunction<String, String>() {
                    ValueState<String> valueState;
                    ListState<String> listState;
                    MapState<String,String> mapState;
                    ReducingState<String> reducingState;
                    AggregatingState<Integer,Double> aggregatingState;//泛型1：进来的数据，泛型2，输出的数据

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        valueState = getRuntimeContext().getState(new ValueStateDescriptor<String>("valueState",String.class));
                        RuntimeContext context = getRuntimeContext();
                        // 获取一个list结构的状态存储器
                        listState = context.getListState(new ListStateDescriptor<String>("name", String.class));
                        reducingState =context.getReducingState(new ReducingStateDescriptor<String>("reduce", new ReduceFunction<String>() {
                            @Override
                            public String reduce(String s, String t1) throws Exception {
                                /**
                                 * @param s 传进来的数据
                                 * @param t1 reduce的结果
                                 * @return java.lang.String
                                 **/
                                return s+""+t1;
                            }
                        },String.class));

                        aggregatingState = context.getAggregatingState(
                                new AggregatingStateDescriptor<Integer, Tuple2<Integer,Integer>, Double>("agg",
                                        new AggregateFunction<Integer, Tuple2<Integer, Integer>, Double>() {
                                    /**
                                     * 泛型1：进来的数据
                                     * 泛型2：累加器
                                     * 泛型3：输出的类型Ouble
                                     **/
                                    @Override
                                    public Tuple2<Integer, Integer> createAccumulator() {
                                        return Tuple2.of(0,0);
                                    }

                                    @Override
                                    public Tuple2<Integer, Integer> add(Integer integer, Tuple2<Integer, Integer> acc) {
                                        return Tuple2.of(acc.f0+1,acc.f1+integer);
                                    }

                                    @Override
                                    public Double getResult(Tuple2<Integer, Integer> accumulator) {
                                        return accumulator.f1/(double)accumulator.f0;
                                    }

                                    @Override
                                    public Tuple2<Integer, Integer> merge(Tuple2<Integer, Integer> acc0, Tuple2<Integer, Integer> acc1) {
                                        return Tuple2.of(acc1.f0+acc0.f0,acc0.f1+acc1.f1);
                                    }
                                }, TypeInformation.of(new TypeHint<Tuple2<Integer, Integer>>() {
                                })));
                        // 获取一个map结构的状态存储器
                        // 获取一个单值结构的状态存储器

                    }
                    @Override
                    public String map(String s) throws Exception {
                        ZonedDateTime now = Instant.now().atZone(ZoneId.of("Asia/Shanghai"));// 获取当前时间带时区
                        DateTimeFormatter dtf1 = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
                        //System.out.println(dtf1.format(now));// 2022-10-03 21:00:45
                        listState.add(s);
                        // listState.update(Arrays.asList("a","b"));先清空再添加
                        StringBuilder builder = new StringBuilder();
                        for (String s1 : listState.get()) {
                            builder.append(s1);
                        }

                        //mapState的操作
                        mapState.contains("a");
                        mapState.get("a");
                        mapState.put("a","value");
                        Iterator<Map.Entry<String, String>> iterator = mapState.iterator();
                        Iterable<Map.Entry<String, String>> entryIterable = mapState.entries();

                        //reduceState,里面只保存了一个值
                        reducingState.add("a");
                        reducingState.get();


                        // agg
                        aggregatingState.add(10);
                        aggregatingState.get();//10.0
                        return dtf1.format(now)+" "+ builder.toString();
                    }
                }).print();

        env.execute();

    }
}
