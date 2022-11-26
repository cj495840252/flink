package org.flink.demos.state;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;


/**
 * 键控状态，通常用于source记录偏移量
 * @author chnejia
 */

public class demo3 {
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
                    ListState<String> listState;
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        RuntimeContext context = getRuntimeContext();
                        // 获取一个list结构的状态存储器
                        listState = context.getListState(new ListStateDescriptor<String>("name", String.class));
                        // 获取一个map结构的状态存储器
                        // 获取一个单值结构的状态存储器

                    }
                    @Override
                    public String map(String s) throws Exception {
                        ZonedDateTime now = Instant.now().atZone(ZoneId.of("Asia/Shanghai"));// 获取当前时间带时区
                        DateTimeFormatter dtf1 = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
                        //System.out.println(dtf1.format(now));// 2022-10-03 21:00:45
                        listState.add(s);
                        StringBuilder builder = new StringBuilder();
                        for (String s1 : listState.get()) {
                            builder.append(s1);
                        }
                        return dtf1.format(now)+" "+ builder.toString();
                    }
                }).print();

        env.execute();
    }
}

