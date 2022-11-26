package org.flink.demos.state;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;

/**
 * 这个文件包含了TTL（time to life：状态的生命周期）的全部操作，且实现了Job级的重启恢复
 * */

public class demo5_TTL {
    public static void main(String[] args) throws Exception {

        System.setProperty("log.file","./webui");
        Configuration conf = new Configuration();
        conf.setString("web.log.path","./webui");
        //conf.setString("execution.savepoint.path","file:///F:/JavaProject/flink-test/mode1/src/outfile/checkpoint/3f5740a390c03d8d77e61cf7247796d2/chk-109");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);
        // 开启checkpoint，默认精确一次
        env.enableCheckpointing(1000, CheckpointingMode.EXACTLY_ONCE);
        //env.getCheckpointConfig().setCheckpointStorage(new URI("hdfs://test"));
        //env.getCheckpointConfig().setCheckpointStorage("file:///F:/JavaProject/flink-test/mode1/src/outfile/checkpoint");


        // 设置backend
        // env.setStateBackend(new HashMapStateBackend());
        // EmbeddedRocksDBStateBackend backend = new EmbeddedRocksDBStateBackend();
        // env.setStateBackend(backend);



        DataStreamSource<String> source = env.socketTextStream("192.168.88.163", 9999);
        source.keyBy(s->"0").map(new RichMapFunction<String, String>() {
            // 1.声明一个状态
            ValueState<String> valueState;
            ListState<String> listState;
            @Override
            public String map(String s) throws Exception {
                listState.add(s);
                Iterable<String> iterable = listState.get();

                StringBuilder sb = new StringBuilder();
                for (String s1 : iterable) {
                    sb.append(s1);
                }
                return sb.toString();
            }

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                ValueStateDescriptor<String> descriptor = new ValueStateDescriptor<>("valueState", String.class);
                ListStateDescriptor<String> descriptor1 = new ListStateDescriptor<>("listDescriptor", String.class);

                StateTtlConfig ttlConfig = StateTtlConfig.newBuilder(Time.milliseconds(5000))
                        .setTtl(Time.milliseconds(4000)) //和上面的参数一样，冗余了
                        //.updateTtlOnCreateAndWrite() // 当插入和更新的时候,刷新ttl计时
                        //.updateTtlOnReadAndWrite()   // 读写时都会刷新ttl计时
                        //.setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)//和上面的返回策略一样
                        // 设置状态的可见性
                        //.setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)// 永不返回已过期的
                        //如果还没有清除，返回过期的数据
                        .setStateVisibility(StateTtlConfig.StateVisibility.ReturnExpiredIfNotCleanedUp)
                        // 将ttl设置处理时间语义,或者.useProcessingTime(),默认的时间就是processingTime
                        .setTtlTimeCharacteristic(StateTtlConfig.TtlTimeCharacteristic.ProcessingTime)
                        //.useProcessingTime();
                        //.cleanupIncrementally(3,true)//增量清理策略,不建议true
                        //.cleanupIncrementally(5,false)//默认设置
                        //.cleanupFullSnapshot()//全量快照清理，checkpoint时保存未过期的，但是不会清理本地数据
                        //状态后端,在rockdb的compact中添加过滤器，compact时清理
                        //.cleanupInRocksdbCompactFilter(1000)
                        //.disableCleanupInBackground()
                        .build();

                descriptor.enableTimeToLive(ttlConfig); //开启TTL管理
                descriptor1.enableTimeToLive(ttlConfig);
                valueState = getRuntimeContext().getState(descriptor);
                listState = getRuntimeContext().getListState(descriptor1);
            }

        }).setParallelism(1).print();

        env.execute();
    }
}
